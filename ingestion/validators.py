from __future__ import annotations

import hashlib
import re
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError, ParserError

from ingestion.models import RowRejection, ValidationIssue, ValidationMetrics, ValidationResult
from ingestion.table_config import TableConfig

_NON_ALPHANUMERIC_PATTERN = re.compile(r"[^0-9a-zA-Z]+")
_MULTIPLE_UNDERSCORES_PATTERN = re.compile(r"_+")


def validate_file_exists(file_path: Path) -> None:
    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")


def compute_file_checksum(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_csv_file(file_path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(
            file_path,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            encoding="utf-8-sig",
        )
    except UnicodeDecodeError as exc:
        raise ValueError(f"ENCODING_ERROR: Unable to decode {file_path.name} as UTF-8.") from exc
    except EmptyDataError as exc:
        raise ValueError(f"EMPTY_FILE: {file_path.name} has no columns or rows.") from exc
    except ParserError as exc:
        raise ValueError(f"SCHEMA_DRIFT: {file_path.name} could not be parsed as CSV.") from exc


def normalize_column_name(column_name: str) -> str:
    normalized = _NON_ALPHANUMERIC_PATTERN.sub("_", column_name.strip().lower())
    normalized = _MULTIPLE_UNDERSCORES_PATTERN.sub("_", normalized).strip("_")
    if not normalized:
        raise ValueError(f"Column name '{column_name}' normalizes to an empty identifier.")
    return normalized


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    normalized_columns = [normalize_column_name(column_name) for column_name in df.columns]

    duplicate_columns = sorted(
        {column_name for column_name in normalized_columns if normalized_columns.count(column_name) > 1}
    )
    if duplicate_columns:
        raise ValueError(
            "Column normalization collision detected for "
            f"{duplicate_columns}. Review source headers before loading."
        )

    normalized_df = df.copy()
    normalized_df.columns = normalized_columns
    return normalized_df


def validate_required_columns(df: pd.DataFrame, required_columns: list[str] | tuple[str, ...]) -> None:
    missing_columns = sorted(set(required_columns) - set(df.columns))
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


def validate_primary_key(df: pd.DataFrame, pk: str | list[str] | tuple[str, ...]) -> None:
    pk_columns = [pk] if isinstance(pk, str) else list(pk)

    missing_pk_columns = sorted(set(pk_columns) - set(df.columns))
    if missing_pk_columns:
        raise ValueError(f"Primary key column not found: {missing_pk_columns}")

    null_mask = pd.Series(False, index=df.index)
    for column_name in pk_columns:
        null_mask |= df[column_name].map(is_missing_value)

    if null_mask.any():
        raise ValueError(f"Primary key columns contain null values: {pk_columns}")


def is_missing_value(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return bool(pd.isna(value))


def validate_table_dataframe(df: pd.DataFrame, table_config: TableConfig) -> ValidationResult:
    try:
        normalized_df = normalize_column_names(df)
    except ValueError as exc:
        return ValidationResult(
            is_valid=False,
            normalized_dataframe=pd.DataFrame(),
            accepted_dataframe=pd.DataFrame(),
            errors=[ValidationIssue(reason_code="DUPLICATED_COLUMN_NAME", detail=str(exc))],
            metrics=ValidationMetrics(),
        )

    rows_read = len(normalized_df)
    if rows_read == 0:
        return ValidationResult(
            is_valid=False,
            normalized_dataframe=normalized_df,
            accepted_dataframe=normalized_df.iloc[0:0].copy(),
            errors=[ValidationIssue(reason_code="EMPTY_FILE", detail="CSV file contains no data rows.")],
            metrics=ValidationMetrics(rows_read=0),
        )

    errors: list[ValidationIssue] = []

    try:
        validate_required_columns(normalized_df, table_config.required_columns)
    except ValueError as exc:
        errors.append(ValidationIssue(reason_code="MISSING_REQUIRED_COLUMN", detail=str(exc)))

    unexpected_columns = sorted(set(normalized_df.columns) - table_config.known_columns)
    if unexpected_columns:
        errors.append(
            ValidationIssue(
                reason_code="UNEXPECTED_COLUMN",
                detail=f"Unexpected columns after normalization: {unexpected_columns}",
            )
        )
        errors.append(
            ValidationIssue(
                reason_code="SCHEMA_DRIFT",
                detail="Unexpected columns detected. Update the source contract or the table configuration.",
            )
        )

    if errors:
        return ValidationResult(
            is_valid=False,
            normalized_dataframe=normalized_df,
            accepted_dataframe=normalized_df.iloc[0:0].copy(),
            errors=errors,
            metrics=ValidationMetrics(rows_read=rows_read),
        )

    working_df = normalized_df.copy()
    for column_name in table_config.all_source_columns:
        if column_name not in working_df.columns:
            working_df[column_name] = ""

    working_df = working_df.loc[:, list(table_config.all_source_columns)].copy()
    working_df["_source_row_number"] = range(1, len(working_df) + 1)

    rejections_by_index: dict[int, RowRejection] = {}

    def reject_rows(row_indices: list[int], reason_code: str, detail_builder) -> None:
        for row_index in row_indices:
            if row_index in rejections_by_index:
                continue
            row = working_df.loc[row_index]
            raw_record = {
                column_name: row[column_name]
                for column_name in table_config.all_source_columns
            }
            rejections_by_index[row_index] = RowRejection(
                source_row_number=int(row["_source_row_number"]),
                reason_code=reason_code,
                reason_detail=detail_builder(row),
                raw_record=raw_record,
            )

    null_pk_mask = pd.Series(False, index=working_df.index)
    for column_name in table_config.primary_key:
        null_pk_mask |= working_df[column_name].map(is_missing_value)
    reject_rows(
        working_df.index[null_pk_mask].tolist(),
        "NULL_PRIMARY_KEY",
        lambda row: f"Primary key columns {table_config.primary_key} cannot be null or blank.",
    )

    available_indices = [index for index in working_df.index if index not in rejections_by_index]
    duplicate_mask = working_df.loc[available_indices, list(table_config.primary_key)].duplicated(keep="first")
    duplicate_indices = duplicate_mask[duplicate_mask].index.tolist()
    reject_rows(
        duplicate_indices,
        "DUPLICATE_PRIMARY_KEY",
        lambda row: (
            "Duplicate primary key encountered for "
            + ", ".join(f"{column}={row[column]}" for column in table_config.primary_key)
        ),
    )

    for column_name in table_config.numeric_columns:
        if column_name not in working_df.columns:
            continue
        nonblank_mask = ~working_df[column_name].map(is_missing_value)
        numeric_values = pd.to_numeric(
            working_df[column_name].where(nonblank_mask, None),
            errors="coerce",
        )
        invalid_numeric_mask = nonblank_mask & numeric_values.isna()
        reject_rows(
            working_df.index[invalid_numeric_mask].tolist(),
            "INVALID_NUMERIC_VALUE",
            lambda row, current_column=column_name: (
                f"Column '{current_column}' must contain a numeric value."
            ),
        )

        if column_name in table_config.non_negative_columns:
            negative_mask = nonblank_mask & numeric_values.notna() & (numeric_values < 0)
            reason_code = "NEGATIVE_PRICE" if "price" in column_name else "INVALID_NUMERIC_VALUE"
            reject_rows(
                working_df.index[negative_mask].tolist(),
                reason_code,
                lambda row, current_column=column_name: (
                    f"Column '{current_column}' cannot contain negative values."
                ),
            )

    for column_name in table_config.timestamp_columns:
        if column_name not in working_df.columns:
            continue
        nonblank_mask = ~working_df[column_name].map(is_missing_value)
        timestamp_values = pd.to_datetime(
            working_df[column_name].where(nonblank_mask, None),
            errors="coerce",
        )
        invalid_timestamp_mask = nonblank_mask & timestamp_values.isna()
        reject_rows(
            working_df.index[invalid_timestamp_mask].tolist(),
            "INVALID_TIMESTAMP",
            lambda row, current_column=column_name: (
                f"Column '{current_column}' must contain a valid timestamp."
            ),
        )

    for column_name, accepted_values in table_config.accepted_values.items():
        if column_name not in working_df.columns:
            continue
        normalized_values = working_df[column_name].astype(str).str.strip().str.lower()
        nonblank_mask = ~working_df[column_name].map(is_missing_value)
        invalid_value_mask = nonblank_mask & ~normalized_values.isin(set(accepted_values))
        reject_rows(
            working_df.index[invalid_value_mask].tolist(),
            "UNSUPPORTED_STATUS",
            lambda row, current_column=column_name: (
                f"Column '{current_column}' contains unsupported value '{row[current_column]}'."
            ),
        )

    rejected_indices = sorted(rejections_by_index.keys())
    accepted_df = working_df.drop(index=rejected_indices).copy()
    duplicate_count = sum(
        rejection.reason_code == "DUPLICATE_PRIMARY_KEY"
        for rejection in rejections_by_index.values()
    )

    if accepted_df.empty:
        return ValidationResult(
            is_valid=False,
            normalized_dataframe=working_df,
            accepted_dataframe=accepted_df,
            errors=[
                ValidationIssue(
                    reason_code="NO_VALID_ROWS",
                    detail="All rows were rejected during validation. Existing table data was left untouched.",
                )
            ],
            row_rejections=[rejections_by_index[index] for index in rejected_indices],
            metrics=ValidationMetrics(
                rows_read=rows_read,
                rows_valid=0,
                rows_rejected=len(rejected_indices),
                duplicate_count=duplicate_count,
            ),
        )

    return ValidationResult(
        is_valid=True,
        normalized_dataframe=working_df,
        accepted_dataframe=accepted_df,
        row_rejections=[rejections_by_index[index] for index in rejected_indices],
        metrics=ValidationMetrics(
            rows_read=rows_read,
            rows_valid=len(accepted_df),
            rows_rejected=len(rejected_indices),
            duplicate_count=duplicate_count,
        ),
    )
