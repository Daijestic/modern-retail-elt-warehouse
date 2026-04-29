# ingestion/load_csv_to_postgres.py

import pandas as pd
import uuid
from datetime import datetime
from pathlib import Path

from db import get_engine
from validators import validate_dataframe
from logger import get_logger
from table_config import TABLE_CONFIG

BASE_DIR = Path(__file__).resolve().parent.parent

logger = get_logger()

def load_table(table_cfg):
    run_id = str(uuid.uuid4())
    start_time = datetime.now()

    file_path = BASE_DIR / f"data/raw/{table_cfg['file']}"

    try:
        with get_engine().begin() as conn:
            logger.info(f"{table_cfg['name']} - reading CSV...")

            if not file_path.exists():
                raise FileNotFoundError(f"{file_path} not found")

            df = pd.read_csv(file_path)
            df[table_cfg["pk"]] = df[table_cfg["pk"]].astype(str)

            validate_dataframe(df, table_cfg["required_columns"])

            df = df.drop_duplicates(subset=[table_cfg["pk"]])

            if df.empty:
                row_count = 0
            else:
                existing_ids = pd.read_sql(
                    f"""
                    SELECT {table_cfg['pk']}
                    FROM raw.{table_cfg['table']}
                    WHERE {table_cfg['pk']} IN ({','.join([f"'{x}'" for x in df[table_cfg["pk"]].tolist()])})
                    """,
                    con=conn
                )

                df = df[~df[table_cfg["pk"]].isin(existing_ids[table_cfg["pk"]])]
                row_count = len(df)

            logger.info(f"Rows to insert: {row_count}")

            # load vào raw schema
            if row_count > 0:
                df.to_sql(
                    table_cfg["table"],
                    con=conn,
                    schema="raw",
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=1000
                )

            status = "SUCCESS"
            error_message = None

            logger.info(f"Inserted {row_count} rows into raw.{table_cfg['table']}")

    except Exception as e:
        status = "FAILED"
        error_message = str(e)
        row_count = 0
        logger.exception(f"{table_cfg['name']} ingestion failed")

    finally:
        end_time = datetime.now()

        # insert log vào ingestion_runs
        log_df = pd.DataFrame([{
            "run_id": run_id,
            "source_name": table_cfg["name"],
            "target_table": f"raw.{table_cfg['table']}",
            "row_count": row_count,
            "started_at": start_time,
            "finished_at": end_time,
            "status": status,
            "error_message": error_message
        }])
        try:
            with get_engine().begin() as conn:
                log_df.to_sql(
                    "ingestion_runs",
                    con=conn,
                    schema="raw",
                    if_exists="append",
                    index=False
                )
        except Exception:
            logger.exception("Failed to log ingestion run")

        logger.info("Logged ingestion run")

def run_all():
    for table in TABLE_CONFIG:
        load_table(table)

if __name__ == "__main__":
    run_all()