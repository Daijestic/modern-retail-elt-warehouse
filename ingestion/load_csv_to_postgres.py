import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from ingestion.db import get_engine
from ingestion.logger import get_logger
from ingestion.table_config import TABLE_CONFIG
from ingestion.validators import (
    normalize_column_names,
    validate_file_exists,
    validate_primary_key,
    validate_required_columns,
)

BASE_DIR = Path(__file__).resolve().parent.parent
logger = get_logger()


def record_ingestion_run(
    run_id: str,
    source_name: str,
    target_table: str,
    row_count: int,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    error_message: str | None,
) -> None:
    log_df = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "source_name": source_name,
                "target_table": target_table,
                "row_count": row_count,
                "started_at": started_at,
                "finished_at": finished_at,
                "status": status,
                "error_message": error_message,
            }
        ]
    )

    engine = get_engine()

    with engine.begin() as conn:
        log_df.to_sql(
            "ingestion_runs",
            con=conn,
            schema="metadata",
            if_exists="append",
            index=False,
        )


def load_table(table_cfg: dict) -> None:
    run_id = str(uuid.uuid4())
    started_at = datetime.now()

    source_name = table_cfg["name"]
    target_table = table_cfg["table"]
    pk = table_cfg["pk"]
    file_path = BASE_DIR / "data" / "raw" / table_cfg["file"]

    row_count = 0
    status = "SUCCESS"
    error_message = None

    logger.info("Starting ingestion for %s", source_name)

    try:
        validate_file_exists(file_path)

        df = pd.read_csv(file_path)
        df = normalize_column_names(df)

        validate_required_columns(df, table_cfg["required_columns"])
        validate_primary_key(df, pk)

        df[pk] = df[pk].astype(str)
        df = df.drop_duplicates(subset=[pk])

        row_count = len(df)

        engine = get_engine()

        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE raw.{target_table};"))

            df.to_sql(
                target_table,
                con=conn,
                schema="raw",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

        logger.info("Finished ingestion for %s with %s rows", source_name, row_count)

    except Exception as exc:
        status = "FAILED"
        error_message = str(exc)
        logger.exception("Ingestion failed for %s", source_name)

    finally:
        finished_at = datetime.now()

        record_ingestion_run(
            run_id=run_id,
            source_name=source_name,
            target_table=f"raw.{target_table}",
            row_count=row_count,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            error_message=error_message,
        )


def run_all() -> None:
    for table_cfg in TABLE_CONFIG:
        load_table(table_cfg)


if __name__ == "__main__":
    run_all()