# Architecture

## Overview

This project implements a batch ELT warehouse for retail analytics.

Current data flow:

```text
CSV Retail Dataset
        ↓
Python ingestion pipeline
        ↓
PostgreSQL raw layer
        ↓
metadata.ingestion_runs
        ↓
dbt staging layer
        ↓
dbt marts layer
        ↓
analytics-ready tables
        ↓
Metabase dashboard, planned
```

## Components

### 1. Source Data

Retail CSV files are used as the source system.

Current source files:

```text
customers.csv
orders.csv
order_items.csv
products.csv
payments.csv
shipments.csv
```

### 2. Ingestion Layer

The ingestion layer is implemented in Python.

Responsibilities:

- validate source files exist
- validate required columns
- normalize column names
- validate primary keys
- load CSV data into PostgreSQL raw tables
- record ingestion status in `metadata.ingestion_runs`
- support idempotent reload using `TRUNCATE + INSERT`

### 3. Raw Layer

Raw tables are stored in PostgreSQL under the `raw` schema.

Current raw tables:

```text
raw.raw_customers
raw.raw_orders
raw.raw_order_items
raw.raw_products
raw.raw_payments
raw.raw_shipments
```

Metadata table:

```text
metadata.ingestion_runs
```

### 4. dbt Staging Layer

The staging layer cleans and standardizes raw data.

Current staging models:

```text
stg_customers
stg_orders
stg_order_items
stg_products
stg_payments
stg_shipments
```

Responsibilities:

- rename columns consistently
- cast dates and timestamps
- cast numeric values
- normalize text values
- prepare clean data for marts

### 5. dbt Marts Layer

The marts layer provides analytics-ready tables.

Core marts:

```text
dim_customers
dim_products
fact_orders
fact_order_items
```

Analytics marts:

```text
mart_daily_revenue
mart_product_performance
mart_delivery_performance
```

### 6. Dashboard Layer

Metabase dashboard integration is planned.

Planned dashboard sections:

- Revenue Overview
- Product Performance
- Delivery Performance
- Data Quality Summary

## Future Architecture Improvements

Planned production-like improvements:

- Airflow orchestration
- GitHub Actions CI/CD
- dbt source freshness
- custom business data tests
- Metabase dashboard
- AWS-ready architecture notes
