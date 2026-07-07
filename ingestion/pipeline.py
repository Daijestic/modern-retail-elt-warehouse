from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.config import get_app_config
from ingestion.db import (
    ensure_database_objects,
    get_engine,
    qualified_name,
    quote_identifier,
    table_row_count,
)
from ingestion.logger import get_logger
from ingestion.models import ExecutionHooks, PipelineSummary, TableLoadResult, ValidationIssue
from ingestion.table_config import TableConfig, get_enabled_table_configs
from ingestion.validators import (
    compute_file_checksum,
    read_csv_file,
    validate_file_exists,
    validate_table_dataframe,
)

LOAD_STRATEGY = "atomic_full_refresh_via_stage_table"

logger = get_logger(__name__)


def _utcnow() -> datetime:
    return datetime.utcnow()


def _format_validation_errors(errors: list[ValidationIssue]) -> str:
    return "; ".join(f"{error.reason_code}: {error.detail}" for error in errors)


def _extract_error_type(exc: Exception) -> str:
    message = str(exc)
    if isinstance(exc, ValueError) and ":" in message:
        candidate = message.split(":", 1)[0].strip()
        if candidate.isupper():
            return candidate
    return type(exc).__name__


def _determine_table_status(rows_rejected: int, failed: bool = False) -> str:
    if failed:
        return "FAILED"
    if rows_rejected > 0:
        return "PARTIAL_SUCCESS"
    return "SUCCESS"


def determine_pipeline_status(table_results: list[TableLoadResult]) -> str:
    if any(result.status == "FAILED" for result in table_results):
        return "FAILED"
    if any(result.status == "PARTIAL_SUCCESS" for result in table_results):
        return "PARTIAL_SUCCESS"
    return "SUCCESS"


def pipeline_exit_code(summary: PipelineSummary) -> int:
    return 1 if summary.status == "FAILED" else 0


def _build_landing_dataframe(
    accepted_dataframe: pd.DataFrame,
    table_config: TableConfig,
    run_id: str,
    source_file: str,
    file_checksum: str,
    source_modified_at: datetime,
    ingested_at: datetime,
) -> pd.DataFrame:
    landing_df = accepted_dataframe.copy()

    if "_source_row_number" not in landing_df.columns:
        landing_df["_source_row_number"] = range(1, len(landing_df) + 1)

    landing_df["_ingestion_run_id"] = run_id
    landing_df["_source_file"] = source_file
    landing_df["_ingested_at"] = ingested_at
    landing_df["_file_checksum"] = file_checksum
    landing_df["_source_modified_at"] = source_modified_at

    return landing_df.loc[:, list(table_config.target_columns)]


def _build_stage_table_name(target_table: str, table_run_id: str) -> str:
    suffix = table_run_id.replace("-", "_")[:12]
    return f"{target_table}__stg_{suffix}"


def _drop_stage_table(engine: Engine, schema_name: str, stage_table_name: str) -> None:
    with engine.begin() as connection:
        connection.execute(text(f"DROP TABLE IF EXISTS {qualified_name(schema_name, stage_table_name)}"))


def _stage_dataframe(
    engine: Engine,
    schema_name: str,
    target_table_name: str,
    stage_table_name: str,
    landing_df: pd.DataFrame,
) -> None:
    target_name = qualified_name(schema_name, target_table_name)
    stage_name = qualified_name(schema_name, stage_table_name)

    with engine.begin() as connection:
        connection.execute(text(f"DROP TABLE IF EXISTS {stage_name}"))
        connection.execute(
            text(
                f"CREATE TABLE {stage_name} (LIKE {target_name} "
                "INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS)"
            )
        )
        landing_df.to_sql(
            stage_table_name,
            con=connection,
            schema=schema_name,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )


def _validate_stage_row_count(
    engine: Engine,
    schema_name: str,
    stage_table_name: str,
    expected_rows: int,
) -> None:
    actual_rows = table_row_count(engine, schema_name, stage_table_name)
    if actual_rows != expected_rows:
        raise RuntimeError(
            f"Stage row count mismatch: expected {expected_rows} rows but found {actual_rows} rows."
        )


def _replace_target_from_stage(
    engine: Engine,
    schema_name: str,
    target_table_name: str,
    stage_table_name: str,
    target_columns: tuple[str, ...],
    execution_hooks: ExecutionHooks | None = None,
) -> None:
    target_name = qualified_name(schema_name, target_table_name)
    stage_name = qualified_name(schema_name, stage_table_name)
    column_list = ", ".join(quote_identifier(column_name) for column_name in target_columns)

    with engine.begin() as connection:
        if execution_hooks and execution_hooks.before_target_replacement:
            execution_hooks.before_target_replacement()

        connection.execute(text(f"LOCK TABLE {target_name} IN ACCESS EXCLUSIVE MODE"))
        connection.execute(text(f"TRUNCATE TABLE {target_name}"))

        if execution_hooks and execution_hooks.after_target_truncate:
            execution_hooks.after_target_truncate()

        connection.execute(
            text(
                f"INSERT INTO {target_name} ({column_list}) "
                f"SELECT {column_list} FROM {stage_name}"
            )
        )


def _load_dataframe_atomically(
    engine: Engine,
    schema_name: str,
    table_config: TableConfig,
    landing_df: pd.DataFrame,
    table_run_id: str,
    execution_hooks: ExecutionHooks | None = None,
) -> None:
    stage_table_name = _build_stage_table_name(table_config.target_table, table_run_id)

    try:
        _stage_dataframe(
            engine=engine,
            schema_name=schema_name,
            target_table_name=table_config.target_table,
            stage_table_name=stage_table_name,
            landing_df=landing_df,
        )
        _validate_stage_row_count(
            engine=engine,
            schema_name=schema_name,
            stage_table_name=stage_table_name,
            expected_rows=len(landing_df),
        )
        _replace_target_from_stage(
            engine=engine,
            schema_name=schema_name,
            target_table_name=table_config.target_table,
            stage_table_name=stage_table_name,
            target_columns=table_config.target_columns,
            execution_hooks=execution_hooks,
        )
    finally:
        _drop_stage_table(engine, schema_name, stage_table_name)


def _record_pipeline_run_start(engine: Engine, run_id: str, started_at: datetime) -> None:
    app_config = get_app_config()
    insert_sql = text(
        f"""
        INSERT INTO {qualified_name(app_config.metadata_schema, 'pipeline_runs')} (
            run_id,
            status,
            started_at,
            finished_at,
            duration_seconds,
            tables_succeeded,
            tables_partial,
            tables_failed
        ) VALUES (
            :run_id,
            'RUNNING',
            :started_at,
            :started_at,
            0,
            0,
            0,
            0
        )
        ON CONFLICT (run_id) DO NOTHING
        """
    )
    with engine.begin() as connection:
        connection.execute(insert_sql, {"run_id": run_id, "started_at": started_at})


def _record_pipeline_run_completion(engine: Engine, summary: PipelineSummary) -> None:
    app_config = get_app_config()
    update_sql = text(
        f"""
        UPDATE {qualified_name(app_config.metadata_schema, 'pipeline_runs')}
        SET status = :status,
            finished_at = :finished_at,
            duration_seconds = :duration_seconds,
            tables_succeeded = :tables_succeeded,
            tables_partial = :tables_partial,
            tables_failed = :tables_failed
        WHERE run_id = :run_id
        """
    )
    with engine.begin() as connection:
        connection.execute(
            update_sql,
            {
                "run_id": summary.run_id,
                "status": summary.status,
                "finished_at": summary.finished_at,
                "duration_seconds": summary.duration_seconds,
                "tables_succeeded": summary.succeeded,
                "tables_partial": summary.partial,
                "tables_failed": summary.failed,
            },
        )


def _record_table_run(engine: Engine, result: TableLoadResult) -> None:
    app_config = get_app_config()
    insert_sql = text(
        f"""
        INSERT INTO {qualified_name(app_config.metadata_schema, 'ingestion_runs')} (
            table_run_id,
            run_id,
            source_name,
            source_file,
            target_schema,
            target_table,
            file_size_bytes,
            file_checksum,
            schema_version,
            load_strategy,
            status,
            rows_read,
            rows_loaded,
            rows_rejected,
            duplicate_count,
            started_at,
            finished_at,
            duration_seconds,
            error_type,
            error_message
        ) VALUES (
            :table_run_id,
            :run_id,
            :source_name,
            :source_file,
            :target_schema,
            :target_table,
            :file_size_bytes,
            :file_checksum,
            :schema_version,
            :load_strategy,
            :status,
            :rows_read,
            :rows_loaded,
            :rows_rejected,
            :duplicate_count,
            :started_at,
            :finished_at,
            :duration_seconds,
            :error_type,
            :error_message
        )
        """
    )
    with engine.begin() as connection:
        connection.execute(
            insert_sql,
            {
                "table_run_id": result.table_run_id,
                "run_id": result.run_id,
                "source_name": result.source_name,
                "source_file": result.source_file,
                "target_schema": result.target_schema,
                "target_table": result.target_table,
                "file_size_bytes": result.file_size_bytes,
                "file_checksum": result.file_checksum,
                "schema_version": result.schema_version,
                "load_strategy": result.load_strategy,
                "status": result.status,
                "rows_read": result.rows_read,
                "rows_loaded": result.rows_loaded,
                "rows_rejected": result.rows_rejected,
                "duplicate_count": result.duplicate_count,
                "started_at": result.started_at,
                "finished_at": result.finished_at,
                "duration_seconds": result.duration_seconds,
                "error_type": result.error_type,
                "error_message": result.error_message,
            },
        )


def _record_rejections(
    engine: Engine,
    run_id: str,
    table_run_id: str,
    source_file: str,
    target_table: str,
    row_rejections,
) -> None:
    if not row_rejections:
        return

    app_config = get_app_config()
    insert_sql = text(
        f"""
        INSERT INTO {qualified_name(app_config.metadata_schema, 'ingestion_rejections')} (
            rejection_id,
            run_id,
            table_run_id,
            source_file,
            target_table,
            source_row_number,
            reason_code,
            reason_detail,
            raw_record,
            rejected_at
        ) VALUES (
            :rejection_id,
            :run_id,
            :table_run_id,
            :source_file,
            :target_table,
            :source_row_number,
            :reason_code,
            :reason_detail,
            CAST(:raw_record AS JSONB),
            :rejected_at
        )
        """
    )
    rejected_at = _utcnow()
    payload = [
        {
            "rejection_id": str(uuid.uuid4()),
            "run_id": run_id,
            "table_run_id": table_run_id,
            "source_file": source_file,
            "target_table": target_table,
            "source_row_number": rejection.source_row_number,
            "reason_code": rejection.reason_code,
            "reason_detail": rejection.reason_detail,
            "raw_record": json.dumps(rejection.raw_record),
            "rejected_at": rejected_at,
        }
        for rejection in row_rejections
    ]
    with engine.begin() as connection:
        connection.execute(insert_sql, payload)


def _persist_table_metadata(engine: Engine, result: TableLoadResult, row_rejections) -> None:
    _record_table_run(engine, result)
    _record_rejections(
        engine=engine,
        run_id=result.run_id,
        table_run_id=result.table_run_id,
        source_file=result.source_file,
        target_table=f"{result.target_schema}.{result.target_table}",
        row_rejections=row_rejections,
    )


def _log_table_result(result: TableLoadResult) -> None:
    log_method = logger.error if result.status == "FAILED" else logger.info
    log_method(
        "table_load_completed",
        extra={
            "run_id": result.run_id,
            "table_run_id": result.table_run_id,
            "source_file": result.source_file,
            "target_table": f"{result.target_schema}.{result.target_table}",
            "status": result.status,
            "rows_read": result.rows_read,
            "rows_loaded": result.rows_loaded,
            "rows_rejected": result.rows_rejected,
            "duplicate_count": result.duplicate_count,
            "duration_seconds": f"{result.duration_seconds:.3f}",
            "error_type": result.error_type or "-",
            "error_message": result.error_message or "-",
        },
    )


def _build_result(
    *,
    run_id: str,
    table_run_id: str,
    table_config: TableConfig,
    source_file: str,
    file_size_bytes: int,
    file_checksum: str,
    rows_read: int,
    rows_loaded: int,
    rows_rejected: int,
    duplicate_count: int,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    error_type: str | None = None,
    error_message: str | None = None,
) -> TableLoadResult:
    app_config = get_app_config()
    duration_seconds = round((finished_at - started_at).total_seconds(), 3)
    return TableLoadResult(
        run_id=run_id,
        table_run_id=table_run_id,
        source_name=table_config.name,
        source_file=source_file,
        target_schema=app_config.raw_schema,
        target_table=table_config.target_table,
        file_size_bytes=file_size_bytes,
        file_checksum=file_checksum,
        schema_version=app_config.schema_version,
        load_strategy=LOAD_STRATEGY,
        status=status,
        rows_read=rows_read,
        rows_loaded=rows_loaded,
        rows_rejected=rows_rejected,
        duplicate_count=duplicate_count,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        error_type=error_type,
        error_message=error_message,
    )


def load_table(
    table_config: TableConfig,
    run_id: str,
    engine: Engine | None = None,
    execution_hooks: ExecutionHooks | None = None,
) -> TableLoadResult:
    app_config = get_app_config()
    active_engine = engine or get_engine()

    table_run_id = str(uuid.uuid4())
    source_path = app_config.source_data_dir / table_config.file_name
    source_file = source_path.name
    started_at = _utcnow()

    file_size_bytes = 0
    file_checksum = ""
    rows_read = 0
    rows_loaded = 0
    rows_rejected = 0
    duplicate_count = 0
    row_rejections = []

    logger.info(
        "table_load_started",
        extra={
            "run_id": run_id,
            "table_run_id": table_run_id,
            "source_file": source_file,
            "target_table": f"{app_config.raw_schema}.{table_config.target_table}",
            "status": "RUNNING",
        },
    )

    try:
        validate_file_exists(source_path)
        file_size_bytes = source_path.stat().st_size
        file_checksum = compute_file_checksum(source_path)
        source_modified_at = datetime.fromtimestamp(source_path.stat().st_mtime)

        source_df = read_csv_file(source_path)
        validation_result = validate_table_dataframe(source_df, table_config)
        rows_read = validation_result.metrics.rows_read
        rows_loaded = validation_result.metrics.rows_valid
        rows_rejected = validation_result.metrics.rows_rejected
        duplicate_count = validation_result.metrics.duplicate_count
        row_rejections = validation_result.row_rejections

        if not validation_result.is_valid:
            raise ValueError(_format_validation_errors(validation_result.errors))

        ingested_at = _utcnow()
        landing_df = _build_landing_dataframe(
            accepted_dataframe=validation_result.accepted_dataframe,
            table_config=table_config,
            run_id=run_id,
            source_file=source_file,
            file_checksum=file_checksum,
            source_modified_at=source_modified_at,
            ingested_at=ingested_at,
        )

        _load_dataframe_atomically(
            engine=active_engine,
            schema_name=app_config.raw_schema,
            table_config=table_config,
            landing_df=landing_df,
            table_run_id=table_run_id,
            execution_hooks=execution_hooks,
        )

        finished_at = _utcnow()
        result = _build_result(
            run_id=run_id,
            table_run_id=table_run_id,
            table_config=table_config,
            source_file=source_file,
            file_size_bytes=file_size_bytes,
            file_checksum=file_checksum,
            rows_read=rows_read,
            rows_loaded=rows_loaded,
            rows_rejected=rows_rejected,
            duplicate_count=duplicate_count,
            started_at=started_at,
            finished_at=finished_at,
            status=_determine_table_status(rows_rejected=rows_rejected),
        )

        _persist_table_metadata(active_engine, result, row_rejections)
        _log_table_result(result)
        return result

    except Exception as exc:
        finished_at = _utcnow()
        error_type = _extract_error_type(exc)
        error_message = str(exc)

        result = _build_result(
            run_id=run_id,
            table_run_id=table_run_id,
            table_config=table_config,
            source_file=source_file,
            file_size_bytes=file_size_bytes,
            file_checksum=file_checksum,
            rows_read=rows_read,
            rows_loaded=0,
            rows_rejected=rows_rejected,
            duplicate_count=duplicate_count,
            started_at=started_at,
            finished_at=finished_at,
            status="FAILED",
            error_type=error_type,
            error_message=error_message,
        )

        try:
            _persist_table_metadata(active_engine, result, row_rejections)
        except Exception:
            logger.exception(
                "table_load_metadata_failed",
                extra={
                    "run_id": run_id,
                    "table_run_id": table_run_id,
                    "source_file": source_file,
                    "target_table": f"{app_config.raw_schema}.{table_config.target_table}",
                    "status": "FAILED",
                    "rows_read": rows_read,
                    "rows_loaded": 0,
                    "rows_rejected": rows_rejected,
                    "duplicate_count": duplicate_count,
                    "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
                    "error_type": error_type,
                    "error_message": error_message,
                },
            )

        _log_table_result(result)
        setattr(exc, "table_load_result", result)
        raise


def _log_pipeline_summary(summary: PipelineSummary) -> None:
    logger.info(
        "pipeline_run_completed",
        extra={
            "run_id": summary.run_id,
            "status": summary.status,
            "duration_seconds": f"{summary.duration_seconds:.3f}",
        },
    )


def run_pipeline(
    table_configs: list[TableConfig] | None = None,
    engine: Engine | None = None,
    execution_hooks_by_table: dict[str, ExecutionHooks] | None = None,
) -> PipelineSummary:
    active_engine = engine or get_engine()
    ensure_database_objects(active_engine)

    run_id = str(uuid.uuid4())
    started_at = _utcnow()
    _record_pipeline_run_start(active_engine, run_id, started_at)

    results: list[TableLoadResult] = []
    execution_hooks_by_table = execution_hooks_by_table or {}

    for table_config in table_configs or get_enabled_table_configs():
        if not table_config.enabled:
            continue
        try:
            result = load_table(
                table_config=table_config,
                run_id=run_id,
                engine=active_engine,
                execution_hooks=execution_hooks_by_table.get(table_config.name),
            )
        except Exception as exc:
            table_result = getattr(exc, "table_load_result", None)
            if table_result is None:
                raise
            results.append(table_result)
        else:
            results.append(result)

    finished_at = _utcnow()
    summary = PipelineSummary(
        run_id=run_id,
        status=determine_pipeline_status(results),
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=round((finished_at - started_at).total_seconds(), 3),
        table_results=results,
    )
    _record_pipeline_run_completion(active_engine, summary)
    _log_pipeline_summary(summary)
    return summary
