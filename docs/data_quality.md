# Data Quality

## Overview

This project applies data quality checks at two levels:

```text
Python ingestion validation
        ↓
dbt model tests
```

The goal is to catch bad data early and prevent unreliable data from reaching analytics marts.

## 1. Python Ingestion Validation

The ingestion pipeline validates data before loading it into PostgreSQL.

Current checks:

- source file exists
- required columns exist
- primary key columns are not null
- composite primary keys are valid
- column names are normalized
- ingestion status is tracked

## 2. Ingestion Metadata

Each ingestion run is recorded in:

```text
metadata.ingestion_runs
```

Tracked fields:

- run_id
- source_name
- target_table
- row_count
- status
- started_at
- finished_at
- error_message

This table helps debug failed loads and verify that each source table was loaded successfully.

## 3. Idempotency

The current ingestion strategy uses:

```text
TRUNCATE target raw table
        ↓
INSERT latest CSV data
```

This makes reruns safe for the current MVP because running the same ingestion again does not duplicate rows in raw tables.

## 4. dbt Tests

Current dbt tests include:

- not_null
- unique
- relationships
- accepted_values

These tests are applied to staging models and marts.

Examples:

- `order_id` should not be null
- `customer_id` should exist in `dim_customers`
- `product_id` should exist in `dim_products`
- `order_status` should contain accepted values
- analytics marts should have non-null business metrics

## 5. Current Quality Coverage

| Layer | Current Checks |
|---|---|
| Raw ingestion | file existence, required columns, primary key nulls |
| Metadata | ingestion status, row counts, error messages |
| Staging | dbt not_null, unique, relationships, accepted_values |
| Marts | dbt not_null, unique, relationships |

## 6. Planned Custom Business Tests

Planned custom dbt tests:

```text
assert_no_negative_revenue
assert_order_total_matches_items
assert_no_future_order_dates
assert_delivered_orders_have_delivery_date
assert_no_orphan_order_items
```

## 7. Future Improvements

Planned improvements:

- dbt source freshness checks
- custom business assertions
- failure scenario documentation
- data quality dashboard
- Airflow task failure handling
- CI/CD checks for pytest and dbt tests
