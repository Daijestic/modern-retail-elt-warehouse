# Data Model

## Current Raw Tables

### raw.raw_customers

**Grain:** One row per customer.

**Primary Key**

```text
customer_id
```

**Important Columns**

```text
customer_id
customer_unique_id
customer_zip_code_prefix
customer_city
customer_state
ingested_at
```

---

### raw.raw_orders

**Grain:** One row per order.

**Primary Key**

```text
order_id
```

**Important Columns**

```text
order_id
customer_id
order_status
order_purchase_timestamp
order_approved_at
order_delivered_customer_date
order_estimated_delivery_date
ingested_at
```

---

### metadata.ingestion_runs

**Grain:** One row per ingestion attempt per source table.

**Primary Key**

```text
run_id
```

**Purpose**

Track pipeline execution status, row count, target table, start/end timestamps, and error messages.

**Important Columns**

```text
run_id
source_name
target_table
row_count
started_at
finished_at
status
error_message
```

---

## Planned Raw Tables

```text
raw.raw_order_items
raw.raw_products
raw.raw_payments
raw.raw_shipments
```

---

## Planned dbt Models

### Staging Layer

```text
stg_customers
stg_orders
stg_order_items
stg_products
stg_payments
stg_shipments
```

### Core Mart Layer

```text
dim_customers
dim_products
fact_orders
fact_order_items
```

### Analytics Mart Layer

```text
mart_daily_revenue
mart_product_performance
mart_delivery_performance
```