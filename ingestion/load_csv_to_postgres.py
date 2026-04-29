# ingestion/load_csv_to_postgres.py

import pandas as pd
import uuid
from datetime import datetime
from pathlib import Path

from db import get_engine
from validators import validate_dataframe
from logger import get_logger

BASE_DIR = Path(__file__).resolve().parent.parent
file_path = BASE_DIR / "data/raw/customers.csv"

logger = get_logger()

def load_customers():
    run_id = str(uuid.uuid4())
    start_time = datetime.now()
    try:
        with get_engine().begin() as conn:
            logger.info("Reading CSV...")
            df = pd.read_csv(file_path)
            validate_dataframe(df)

            df = df.drop_duplicates(subset=["customer_id"])

            existing_ids = pd.read_sql(
                f"""
                SELECT customer_id
                FROM raw.raw_customers
                WHERE customer_id IN ({','.join(map(str, df['customer_id'].tolist()))})
                """,
                con=conn
            )

            df = df[~df["customer_id"].isin(existing_ids["customer_id"])]

            row_count = len(df)

            logger.info(f"Rows to insert: {row_count}")

            # load vào raw schema
            df.to_sql(
                "raw_customers",
                con=conn,
                schema="raw",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000
            )

            status = "SUCCESS"
            error_message = None

            logger.info(f"Inserted {row_count} rows into raw.raw_customers")

    except Exception as e:
        status = "FAILED"
        error_message = str(e)
        row_count = 0
        logger.exception("Ingestion failed")
        logger.error(f"Error: {error_message}")

    finally:
        end_time = datetime.now()

        # insert log vào ingestion_runs
        log_df = pd.DataFrame([{
            "run_id": run_id,
            "source_name": "customers_csv",
            "target_table": "raw.raw_customers",
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


if __name__ == "__main__":
    load_customers()