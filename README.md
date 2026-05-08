# Modern Retail ELT Warehouse

Production-like retail ELT warehouse built with Python, PostgreSQL, Docker, and dbt.

This project focuses on building a reliable ingestion and warehouse foundation for retail analytics, including validation, idempotent loading, ingestion tracking, and analytics-ready modeling.

---

# Business Problem

Retail teams need reliable analytics for revenue, customer retention, product performance, and delivery operations.

However, raw operational data is often inconsistent, duplicated, or missing required fields.

This project builds a production-like ELT warehouse that:

- ingests raw retail CSV data
- validates schema and required columns
- tracks ingestion runs
- loads data into PostgreSQL raw layer
- prepares the foundation for dbt transformations and analytics marts

---

# Current Project Status (After Week 2)

## Completed

- Python ingestion pipeline
- PostgreSQL raw schema
- Config-driven multi-table loading
- Required column validation
- Primary key null validation
- Column normalization
- Idempotent reload strategy
- Ingestion run tracking
- Basic pytest coverage
- Dockerized local PostgreSQL environment

## In Progress

- dbt staging models
- Docker Compose improvements
- Metabase integration

## Planned

- dbt marts and star schema
- Data quality tests
- Airflow orchestration
- GitHub Actions CI/CD
- AWS-ready architecture notes

---

# Tech Stack

| Category | Tools |
|---|---|
| Language | Python 3.11 |
| Database | PostgreSQL |
| Data Processing | pandas |
| ORM / DB Access | SQLAlchemy |
| Containerization | Docker Compose |
| Transformation | dbt Core |
| Testing | pytest |
| BI | Metabase |
| Orchestration | Airflow (planned) |
| CI/CD | GitHub Actions (planned) |

---

# Architecture

```text
CSV Retail Dataset
        ↓
Python ingestion pipeline
        ↓
Validation and normalization
        ↓
PostgreSQL raw layer
        ↓
Ingestion metadata tracking
        ↓
dbt staging/intermediate/marts
        ↓
Analytics dashboards
```

---

# Project Structure

```text
modern-retail-elt-warehouse/
│
├── ingestion/
│   ├── load_csv_to_postgres.py
│   ├── validators.py
│   ├── config.py
│   ├── db.py
│   ├── logger.py
│   └── table_config.py
│
├── tests/
│   ├── test_validators.py
│   ├── test_config.py
│   └── test_load_csv.py
│
├── sql_practice/
│
├── dbt/
│
├── docs/
│
├── docker-compose.yml
├── Makefile
├── requirements.txt
└── README.md
```

---

# Data Ingestion Design

The ingestion layer loads raw retail CSV files into PostgreSQL raw tables.

## Current Features

- Config-driven ingestion using `TABLE_CONFIG`
- Multi-table loading support
- Required column validation
- Primary key null validation
- Column name normalization
- Idempotent reload strategy (`TRUNCATE + INSERT`)
- Ingestion metadata tracking
- Structured logging
- Basic unit testing with pytest

---

# Raw Tables

Current raw tables:

```text
metadata.raw_customers
metadata.raw_orders
```

Metadata tables:

```text
raw.ingestion_runs
```

---

# Ingestion Flow

```text
validate file exists
        ↓
read CSV with pandas
        ↓
validate required columns
        ↓
normalize column names
        ↓
validate primary keys
        ↓
truncate target table
        ↓
batch insert into PostgreSQL
        ↓
record ingestion run metadata
        ↓
log success/failure
```

---

# Example Ingestion Metadata

```sql
SELECT *
FROM raw.ingestion_runs
ORDER BY started_at DESC;
```

Tracked fields include:

- run_id
- source_name
- target_table
- row_count
- status
- started_at
- finished_at
- error_message

---

# Local Development Setup

## 1. Start PostgreSQL

```bash
make up
```

## 2. Install dependencies

```bash
make install
```

## 3. Run ingestion pipeline

```bash
make load
```

## 4. Run tests

```bash
make test
```

---

# Validate Loaded Data

```sql
SELECT count(*) FROM raw.raw_customers;

SELECT count(*) FROM raw.raw_orders;

SELECT *
FROM raw.ingestion_runs
ORDER BY started_at DESC;
```

---

# Testing

Current test coverage includes:

- Required column validation
- Table configuration validation
- CSV loading validation
- Primary key validation

Run tests:

```bash
pytest
```

---

# Engineering Practices

This project currently implements:

- Config-driven ingestion
- Structured logging
- Validation before loading
- Idempotent reload strategy
- Metadata tracking
- Reproducible local environment
- Basic automated testing

Future improvements:

- Incremental loading
- Advanced deduplication
- dbt source freshness
- Data quality assertions
- Airflow orchestration
- CI/CD pipelines

---

# Roadmap

## Phase 1 - Ingestion Foundation ✅

- Python ingestion
- PostgreSQL raw layer
- Validation
- Logging
- Testing

## Phase 2 - Warehouse Modeling (Current)

- dbt staging models
- dbt intermediate models
- marts and star schema

## Phase 3 - Production Features

- Airflow DAGs
- GitHub Actions CI
- Data quality gates
- Monitoring
- AWS-ready architecture

---

# How to Run End-to-End

```bash
make reset
make load
make test
```

---

# Future Improvements

- dbt snapshots (SCD Type 2)
- Incremental models
- Source freshness monitoring
- Metabase dashboards
- Airflow orchestration
- GitHub Actions CI/CD
- AWS deployment notes

---

# Author

Built as part of a Data Engineering portfolio project focused on production-style ELT workflows and analytics engineering.