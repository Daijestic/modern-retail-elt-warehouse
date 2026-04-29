# ingestion/load_csv_to_postgres.py

import pandas as pd
import uuid
from datetime import datetime

from db import get_engine
from logger import get_logger

logger = get_logger()

def load_customers():
    run_id = str(uuid.uuid4())
    start_time = datetime.now()

    engine = get_engine()

    try:
        logger.info("Reading CSV...")
        df = pd.read_csv("data/raw/customers.csv")

        row_count = len(df)

        logger.info(f"Rows to insert: {row_count}")

        # load vào raw schema
        df.to_sql(
            "raw_customers",
            engine,
            schema="raw",
            if_exists="append",
            index=False
        )

        status = "SUCCESS"
        error_message = None

        logger.info("Load success!")

    except Exception as e:
        status = "FAILED"
        error_message = str(e)
        row_count = 0

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

        log_df.to_sql(
            "ingestion_runs",
            engine,
            schema="raw",
            if_exists="append",
            index=False
        )

        logger.info("Logged ingestion run")


if __name__ == "__main__":
    load_customers()