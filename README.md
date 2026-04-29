# Modern Retail ELT Warehouse

## Business Problem

Retail teams need reliable analytics for revenue, customer retention, product performance, and delivery performance. This project builds a production-like ELT warehouse that ingests raw retail data, transforms it into analytics-ready models, validates data quality, and serves business dashboards.

## Tech Stack

- Python
- PostgreSQL
- Docker Compose
- dbt Core
- Metabase
- Airflow
- GitHub Actions

## Architecture

CSV Retail Dataset  
→ Python ingestion  
→ PostgreSQL raw layer  
→ dbt staging/intermediate/marts  
→ Data quality tests  
→ Metabase dashboard

## Data Ingestion

This project implements a config-driven ingestion pipeline that loads multiple CSV files into a PostgreSQL raw layer.

Key features:

- Multi-table ingestion using configuration
- Schema validation per table
- Idempotent loading (deduplication against existing records)
- Batch insert for performance
- Ingestion logging with run tracking
