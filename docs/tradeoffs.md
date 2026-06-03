# Trade-offs

This document explains key design trade-offs in the current MVP.

## 1. TRUNCATE + INSERT for Raw Ingestion

### Current Decision

The ingestion pipeline currently uses:

```text
TRUNCATE raw table
        ↓
reload CSV
        ↓
record ingestion run
```

### Why This Was Chosen

This strategy is simple and suitable for the current MVP because:

- source data is small
- CSV files are loaded in batch mode
- rerunning the pipeline should not duplicate rows
- implementation is easy to understand and test

### Trade-off

Advantages:

- simple idempotency
- easy to debug
- avoids duplicate records after reruns
- good for small batch datasets

Limitations:

- not efficient for large datasets
- does not preserve raw history
- not suitable for near-real-time ingestion
- cannot easily detect row-level changes

### Future Improvement

For larger datasets, this can be replaced with:

- incremental loading
- upsert/merge strategy
- partition-based reload
- raw history table with ingestion timestamp

---

## 2. PostgreSQL as the Warehouse

### Current Decision

PostgreSQL is used as the local warehouse.

### Why This Was Chosen

PostgreSQL is suitable for this project because:

- easy to run locally with Docker
- supports SQL analytics
- works well with dbt
- familiar for intern/fresher-level Data Engineering projects

### Trade-off

Advantages:

- simple local setup
- low cost
- good SQL support
- easy to demonstrate

Limitations:

- not designed for very large analytical workloads
- less scalable than cloud warehouses such as BigQuery, Snowflake, or Redshift

### Future Improvement

Possible production targets:

- Amazon Redshift
- BigQuery
- Snowflake
- AWS Athena over S3

---

## 3. dbt for Transformations

### Current Decision

dbt is used for SQL transformations from raw data to staging and marts.

### Why This Was Chosen

dbt is suitable because:

- SQL transformations are easy to review
- model dependencies are managed with `ref()`
- tests can be defined close to models
- documentation and lineage can be generated

### Trade-off

Advantages:

- clear transformation layers
- good testing support
- good analytics engineering practice

Limitations:

- requires SQL discipline
- not ideal for complex Python-based transformations
- needs a warehouse connection to run

---

## 4. Basic Marts Before Airflow

### Current Decision

The project builds dbt staging and marts before adding Airflow.

### Why This Was Chosen

The priority is to first build a working ELT warehouse:

```text
ingestion
        ↓
raw tables
        ↓
staging models
        ↓
marts
```

Airflow should orchestrate a pipeline that already works manually.

### Trade-off

Advantages:

- faster MVP
- easier debugging
- better for interview preparation
- avoids adding orchestration too early

Limitations:

- pipeline is still manually triggered
- no scheduling yet
- no retry policy yet

### Future Improvement

Add Airflow DAG after marts and data quality checks are stable.
