from pathlib import Path

import pandas as pd


def validate_file_exists(file_path: Path) -> None:
    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [col.strip().lower() for col in df.columns]
    return df


def validate_required_columns(df: pd.DataFrame, required_columns: list[str]) -> None:
    missing_columns = set(required_columns) - set(df.columns)

    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")


def validate_primary_key(df: pd.DataFrame, pk: str) -> None:
    if pk not in df.columns:
        raise ValueError(f"Primary key column not found: {pk}")

    if df[pk].isna().any():
        raise ValueError(f"Primary key column contains null values: {pk}")