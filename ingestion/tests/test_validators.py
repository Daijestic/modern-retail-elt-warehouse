from pathlib import Path

import pandas as pd
import pytest

from ingestion.validators import (
    normalize_column_names,
    validate_file_exists,
    validate_primary_key,
    validate_required_columns,
)


def test_validate_file_exists_raises_error_for_missing_file():
    with pytest.raises(FileNotFoundError):
        validate_file_exists(Path("not_exist.csv"))


def test_normalize_column_names():
    df = pd.DataFrame(
        {
            " Customer_ID ": ["c1"],
            "Customer_City": ["hanoi"],
        }
    )

    normalized_df = normalize_column_names(df)

    assert list(normalized_df.columns) == ["customer_id", "customer_city"]


def test_validate_required_columns_passes_when_columns_exist():
    df = pd.DataFrame(
        {
            "customer_id": ["c1"],
            "customer_city": ["hanoi"],
        }
    )

    validate_required_columns(df, ["customer_id", "customer_city"])


def test_validate_required_columns_raises_error_when_missing_column():
    df = pd.DataFrame(
        {
            "customer_id": ["c1"],
        }
    )

    with pytest.raises(ValueError):
        validate_required_columns(df, ["customer_id", "customer_city"])


def test_validate_primary_key_raises_error_when_null():
    df = pd.DataFrame(
        {
            "customer_id": [None],
        }
    )

    with pytest.raises(ValueError):
        validate_primary_key(df, "customer_id")