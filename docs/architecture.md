# Architecture

## Current MVP Architecture

```text
CSV files
  ↓
Python ingestion
  ↓
Validation and normalization
  ↓
PostgreSQL raw layer
  ↓
Ingestion metadata tracking
  ↓
SQL/dbt modeling layer
  ↓
Analytics marts and dashboard
```

---
# Current Implemented Components
|Component|	Status|	Notes|
|---|---|---|
|CSV ingestion|	Completed|	Implemented with Python and pandas|
|Raw PostgreSQL schema|	Completed|	raw.raw_customers, raw.raw_orders|
|Ingestion metadata|	Completed|	metadata.ingestion_runs|
|Required column validation|	Completed|	Validates source schema before loading|
|Primary key validation|	Completed|	Checks null primary keys|
|Duplicate handling|	Completed|	Keeps last record by primary key|
|dbt staging|	In progress| To be implemented next|
|dbt marts|	Planned|	Star schema and analytics marts|
|Dashboard|	Planned|	Metabase dashboard|
---

# Schema Separation
- raw: stores raw source data loaded from CSV files.
- metadata: stores pipeline operational metadata such as ingestion run logs.

---
# Why This Design?
This design separates ingestion, storage, validation, and analytics modeling. It makes the pipeline easier to debug, test, and extend.

---