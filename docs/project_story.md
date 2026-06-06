# Project Story - Modern Retail ELT Warehouse

## 1. Short Introduction

Modern Retail ELT Warehouse is a portfolio project that simulates a batch ELT data pipeline for retail analytics.

The project ingests raw retail CSV data into PostgreSQL, validates data quality at ingestion time, tracks ingestion runs, and uses dbt to transform raw data into clean staging models and analytics-ready marts.

The final marts support common business questions around revenue, product performance, and delivery performance.

## 2. Business Problem

Retail teams need reliable analytics for revenue, customer behavior, product performance, and delivery operations.

Raw operational data is often duplicated, inconsistent, missing required fields, or difficult to query directly. This project solves that by building a small but structured ELT warehouse with clear data layers and validation.

## 3. End-to-End Pipeline

CSV retail data
→ Python ingestion
→ PostgreSQL raw layer
→ metadata.ingestion_runs
→ dbt staging models
→ dbt marts
→ SQL quality checks
→ analytics outputs and screenshots

## 4. What I Implemented

- Config-driven CSV ingestion using Python.
- Required column validation before loading.
- Primary key and composite primary key validation.
- Column name normalization.
- Idempotent reload strategy using TRUNCATE + INSERT.
- Ingestion run tracking with row count, status, start time, finish time, and error message.
- dbt staging models for cleaned source tables.
- dbt core marts including dimension and fact tables.
- Analytics marts for daily revenue, product performance, and delivery performance.
- dbt tests and manual SQL data quality checks.
- Documentation, screenshots, and interview notes.

## 5. Key Engineering Decisions

### Why Python ingestion?

Python gives flexibility for reading CSV files, validating required columns, handling errors, logging pipeline status, and loading data into PostgreSQL.

### Why PostgreSQL?

PostgreSQL is lightweight, popular, easy to run with Docker, and suitable for practicing warehouse-style SQL, dbt models, and analytics queries.

### Why dbt?

dbt helps organize SQL transformations into clear layers, manage dependencies with ref(), and add data tests such as not_null, unique, relationships, and accepted_values.

### Why raw, staging, and marts?

Raw keeps the original loaded data. Staging standardizes and cleans data. Marts are analytics-ready tables designed for business reporting and dashboarding.

## 6. Data Quality Strategy

The project includes data quality checks at multiple levels:

- Ingestion validation: required columns, primary keys, input file existence.
- dbt tests: not_null, unique, relationships, accepted_values.
- SQL checks: duplicates, nulls, orphan records, negative values, and mart sanity checks.

## 7. Current Limitations

The current MVP does not yet include Airflow orchestration, GitHub Actions CI/CD, Metabase dashboard, AWS deployment notes, SCD Type 2 snapshots, or incremental models.

These are planned future improvements after the MVP is stable.

## 8. Interview Pitch - 60 Seconds

This project is a Modern Retail ELT Warehouse MVP. I used Python to ingest raw retail CSV data into PostgreSQL, with validation for required columns, primary keys, column normalization, logging, and ingestion run tracking. Then I used dbt to transform raw data into staging models and analytics-ready marts such as daily revenue, product performance, and delivery performance. I also added dbt tests and manual SQL quality checks to detect nulls, duplicates, orphan records, and invalid business data. The goal of the project is to show a practical ELT workflow with data quality, clear modeling layers, and business-facing analytics outputs.