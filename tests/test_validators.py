from pathlib import Path

import pandas as pd
import pytest

from ingestion.validators import (
    normalize_column_names,
    validate_file_exists,
    validate_primary_key,
    validate_required_columns,
)


def test_validate_file_exists_raises_for_missing_file(tmp_path: Path):
    missing_file = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError):
        validate_file_exists(missing_file)


def test_normalize_column_names():
    df = pd.DataFrame({" Customer_ID ": [1], "Order Status": ["delivered"]})

    result = normalize_column_names(df)

    assert list(result.columns) == ["customer_id", "order status"]


def test_validate_required_columns_passes():
    df = pd.DataFrame({"customer_id": [1], "customer_unique_id": [2]})

    validate_required_columns(df, ["customer_id", "customer_unique_id"])


def test_validate_required_columns_raises_for_missing_column():
    df = pd.DataFrame({"customer_id": [1]})

    with pytest.raises(ValueError, match="Missing required columns"):
        validate_required_columns(df, ["customer_id", "customer_unique_id"])


def test_validate_primary_key_raises_for_null_pk():
    df = pd.DataFrame({"customer_id": [None, "c2"]})

    with pytest.raises(ValueError, match="contains null values"):
        validate_primary_key(df, "customer_id")