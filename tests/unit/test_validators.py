from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from ingestion.table_config import get_table_config
from ingestion.validators import (
    compute_file_checksum,
    normalize_column_names,
    validate_file_exists,
    validate_primary_key,
    validate_required_columns,
    validate_table_dataframe,
)


def test_validate_file_exists_raises_for_missing_file(tmp_path: Path):
    missing_file = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError):
        validate_file_exists(missing_file)


def test_compute_file_checksum_is_stable(tmp_path: Path):
    csv_file = tmp_path / "customers.csv"
    csv_file.write_text("customer_id\ncust_001\n", encoding="utf-8")

    checksum_one = compute_file_checksum(csv_file)
    checksum_two = compute_file_checksum(csv_file)

    assert checksum_one == checksum_two
    assert len(checksum_one) == 64


def test_normalize_column_names_applies_safe_snake_case():
    dataframe = pd.DataFrame({" Customer ID ": [1], "Order Status": ["delivered"]})

    result = normalize_column_names(dataframe)

    assert list(result.columns) == ["customer_id", "order_status"]


def test_normalize_column_names_detects_collisions():
    dataframe = pd.DataFrame({"Order Status": ["delivered"], "order-status": ["shipped"]})

    with pytest.raises(ValueError, match="collision"):
        normalize_column_names(dataframe)


def test_validate_required_columns_raises_for_missing_column():
    dataframe = pd.DataFrame({"customer_id": [1]})

    with pytest.raises(ValueError, match="Missing required columns"):
        validate_required_columns(dataframe, ["customer_id", "customer_unique_id"])


def test_validate_primary_key_raises_for_null_pk():
    dataframe = pd.DataFrame({"customer_id": ["", "c2"]})

    with pytest.raises(ValueError, match="Primary key columns contain null values"):
        validate_primary_key(dataframe, "customer_id")


def test_validate_table_dataframe_returns_structured_row_rejections():
    dataframe = pd.DataFrame(
        [
            {
                "order_id": "ord_001",
                "order_item_id": "1",
                "product_id": "prd_001",
                "seller_id": "sel_001",
                "price": "100.00",
                "freight_value": "10.00",
            },
            {
                "order_id": "ord_001",
                "order_item_id": "1",
                "product_id": "prd_002",
                "seller_id": "sel_002",
                "price": "120.00",
                "freight_value": "15.00",
            },
            {
                "order_id": "ord_002",
                "order_item_id": "2",
                "product_id": "prd_003",
                "seller_id": "sel_003",
                "price": "-5.00",
                "freight_value": "8.00",
            },
        ]
    )

    result = validate_table_dataframe(dataframe, get_table_config("order_items"))

    assert result.is_valid
    assert result.metrics.rows_read == 3
    assert result.metrics.rows_valid == 1
    assert result.metrics.rows_rejected == 2
    assert result.metrics.duplicate_count == 1
    assert {rejection.reason_code for rejection in result.row_rejections} == {
        "DUPLICATE_PRIMARY_KEY",
        "NEGATIVE_PRICE",
    }


def test_validate_table_dataframe_fails_for_unexpected_columns():
    dataframe = pd.DataFrame(
        [
            {
                "order_id": "ord_001",
                "customer_id": "cust_001",
                "order_status": "delivered",
                "order_purchase_timestamp": "2025-01-01 10:00:00",
                "extra_column": "boom",
            }
        ]
    )

    result = validate_table_dataframe(dataframe, get_table_config("orders"))

    assert not result.is_valid
    assert {error.reason_code for error in result.errors} == {"UNEXPECTED_COLUMN", "SCHEMA_DRIFT"}


def test_validate_table_dataframe_fails_for_invalid_timestamps():
    dataframe = pd.DataFrame(
        [
            {
                "order_id": "ord_001",
                "delivered_customer_date": "bad-date",
                "estimated_delivery_date": "2025-01-04 12:00:00",
            }
        ]
    )

    result = validate_table_dataframe(dataframe, get_table_config("shipments"))

    assert not result.is_valid
    assert result.metrics.rows_rejected == 1
    assert result.row_rejections[0].reason_code == "INVALID_TIMESTAMP"
