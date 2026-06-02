# Data Quality Strategy

## Current Checks

The current ingestion layer implements these checks before loading data into PostgreSQL:

| Check                       | Purpose                                      |
| --------------------------- | -------------------------------------------- |
| File existence check        | Prevent loading missing input files          |
| Required column validation  | Ensure source files contain expected columns |
| Primary key null validation | Prevent invalid primary keys                 |
| Column name normalization   | Standardize column names before loading      |
| Duplicate handling          | Avoid duplicate primary keys in raw tables   |
| Ingestion run tracking      | Audit success/failure and row counts         |

---

## Current SQL Quality Checks

### Duplicate Customers

```sql
SELECT customer_id, COUNT(*) AS record_count
FROM raw.raw_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

### Duplicate Orders

```sql
SELECT order_id, COUNT(*) AS record_count
FROM raw.raw_orders
GROUP BY order_id
HAVING COUNT(*) > 1;
```

### Orders with Missing Customer IDs

```sql
SELECT *
FROM raw.raw_orders
WHERE customer_id IS NULL;
```

### Orphan Orders

Orders that reference a customer that does not exist in `raw.raw_customers`.

```sql
SELECT o.*
FROM raw.raw_orders AS o
LEFT JOIN raw.raw_customers AS c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
```

---

## Planned dbt Tests

### Generic Tests

```text
not_null
unique
relationships
accepted_values
```

### Custom Business Tests

```text
assert_no_negative_revenue
assert_delivered_orders_have_delivered_at
assert_order_total_matches_items
assert_no_future_order_dates
assert_customer_key_not_orphaned
```

### Source Freshness

```text
Monitor source data freshness
Detect stale upstream data
Fail pipeline when freshness thresholds are exceeded
```

---

## Future Data Quality Improvements

```text
dbt source freshness checks
20+ dbt tests
Custom business validation rules
Airflow quality gates
Automated failure reporting
Data quality documentation
```
