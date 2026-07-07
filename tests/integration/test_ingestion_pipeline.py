from __future__ import annotations

import pytest
from sqlalchemy import text

from ingestion.models import ExecutionHooks
from ingestion.pipeline import load_table, run_pipeline
from ingestion.table_config import get_table_config

from tests.helpers import build_valid_dataset, query_scalar, write_dataset


@pytest.mark.integration
def test_run_pipeline_loads_all_configured_tables(clean_database, source_data_dir):
    summary = run_pipeline(engine=clean_database)

    assert summary.status == "SUCCESS"
    assert len(summary.table_results) == 6
    assert query_scalar(clean_database, "SELECT COUNT(*) FROM metadata.ingestion_runs") == 6
    assert query_scalar(clean_database, "SELECT COUNT(*) FROM raw.raw_orders") == 2
    assert query_scalar(clean_database, "SELECT COUNT(*) FROM raw.raw_order_items") == 3
    assert query_scalar(clean_database, "SELECT COUNT(*) FROM metadata.pipeline_runs WHERE status = 'SUCCESS'") == 1


@pytest.mark.integration
def test_run_pipeline_records_rejections_without_loading_bad_rows(clean_database, source_data_dir):
    dataset = build_valid_dataset()
    dataset["order_items.csv"].append(
        {
            "order_id": "ord_002",
            "order_item_id": "1",
            "product_id": "prd_999",
            "seller_id": "sel_009",
            "shipping_limit_date": "2025-01-04 10:00:00",
            "price": "-10.00",
            "freight_value": "7.00",
        }
    )
    write_dataset(source_data_dir, dataset)

    summary = run_pipeline(engine=clean_database)

    assert summary.status == "PARTIAL_SUCCESS"
    assert query_scalar(clean_database, "SELECT COUNT(*) FROM raw.raw_order_items") == 3
    assert query_scalar(clean_database, "SELECT COUNT(*) FROM metadata.ingestion_rejections") == 1
    assert query_scalar(
        clean_database,
        "SELECT reason_code FROM metadata.ingestion_rejections LIMIT 1",
    ) == "DUPLICATE_PRIMARY_KEY"
    assert query_scalar(
        clean_database,
        "SELECT rows_rejected FROM metadata.ingestion_runs WHERE source_name = 'order_items' ORDER BY finished_at DESC LIMIT 1",
    ) == 1


@pytest.mark.integration
def test_repeated_load_is_idempotent(clean_database, source_data_dir):
    first_summary = run_pipeline(engine=clean_database)
    second_summary = run_pipeline(engine=clean_database)

    assert first_summary.status == "SUCCESS"
    assert second_summary.status == "SUCCESS"
    assert query_scalar(clean_database, "SELECT COUNT(*) FROM raw.raw_customers") == 2
    assert query_scalar(
        clean_database,
        """
        SELECT COUNT(*) FROM (
            SELECT customer_id, COUNT(*)
            FROM raw.raw_customers
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        ) duplicates
        """,
    ) == 0


@pytest.mark.integration
def test_failure_before_target_replacement_preserves_previous_data(clean_database, source_data_dir):
    customers_config = get_table_config("customers")
    load_table(customers_config, run_id="baseline-run", engine=clean_database)

    original_customer_count = query_scalar(clean_database, "SELECT COUNT(*) FROM raw.raw_customers")
    dataset = build_valid_dataset()
    dataset["customers.csv"] = [
        {
            "customer_id": "cust_003",
            "customer_unique_id": "uniq_003",
            "customer_zip_code_prefix": "55000",
            "customer_city": "da_nang",
            "customer_state": "DN",
        }
    ]
    write_dataset(source_data_dir, dataset)

    def raise_before_replacement():
        raise RuntimeError("failure before target replacement")

    with pytest.raises(RuntimeError, match="failure before target replacement"):
        load_table(
            customers_config,
            run_id="failing-run-before",
            engine=clean_database,
            execution_hooks=ExecutionHooks(before_target_replacement=raise_before_replacement),
        )

    assert query_scalar(clean_database, "SELECT COUNT(*) FROM raw.raw_customers") == original_customer_count
    assert query_scalar(
        clean_database,
        """
        SELECT status
        FROM metadata.ingestion_runs
        WHERE run_id = 'failing-run-before'
        ORDER BY finished_at DESC
        LIMIT 1
        """,
    ) == "FAILED"


@pytest.mark.integration
def test_failure_during_target_replacement_preserves_previous_data(clean_database, source_data_dir):
    customers_config = get_table_config("customers")
    load_table(customers_config, run_id="baseline-run", engine=clean_database)

    dataset = build_valid_dataset()
    dataset["customers.csv"] = [
        {
            "customer_id": "cust_004",
            "customer_unique_id": "uniq_004",
            "customer_zip_code_prefix": "18000",
            "customer_city": "hai_phong",
            "customer_state": "HP",
        }
    ]
    write_dataset(source_data_dir, dataset)

    def raise_after_truncate():
        raise RuntimeError("failure during target replacement")

    with pytest.raises(RuntimeError, match="failure during target replacement"):
        load_table(
            customers_config,
            run_id="failing-run-during",
            engine=clean_database,
            execution_hooks=ExecutionHooks(after_target_truncate=raise_after_truncate),
        )

    rows = []
    with clean_database.connect() as connection:
        result = connection.execute(
            text("SELECT customer_id FROM raw.raw_customers ORDER BY customer_id")
        )
        rows = [row[0] for row in result.fetchall()]

    assert rows == ["cust_001", "cust_002"]
    assert query_scalar(
        clean_database,
        """
        SELECT status
        FROM metadata.ingestion_runs
        WHERE run_id = 'failing-run-during'
        ORDER BY finished_at DESC
        LIMIT 1
        """,
    ) == "FAILED"
