# Modern Retail ELT Warehouse

Production-style retail ELT warehouse built with **Python, PostgreSQL, Docker, and dbt**.

This project focuses on building a reliable batch ELT pipeline for retail analytics, including raw data ingestion, validation, idempotent loading, ingestion tracking, dbt staging models, dbt marts, data quality tests, SQL-based data quality checks, and analytics-ready tables for revenue, product, and delivery reporting.

---

## 1. Business Problem

Retail teams need reliable analytics for revenue, customer behavior, product performance, and delivery operations.

However, raw operational data is often inconsistent, duplicated, missing required fields, or difficult to query directly for business reporting.

This project builds a modern ELT warehouse that:

* ingests raw retail CSV data into PostgreSQL
* validates required columns and primary keys before loading
* tracks every ingestion run with row counts and status
* transforms raw data into clean dbt staging models
* builds analytics-ready marts for revenue, product, and delivery analysis
* applies dbt tests and SQL quality checks to improve data reliability
* prepares the warehouse foundation for future dashboarding and orchestration

---

## 2. Current Project Status

### Completed

- Python ingestion pipeline
- PostgreSQL raw schema
- Config-driven multi-table loading
- Required column validation
- Primary key null validation
- Composite primary key validation
- Column name normalization
- Idempotent reload strategy using `TRUNCATE + INSERT`
- Ingestion run tracking
- Structured logging
- Basic pytest coverage
- Dockerized local PostgreSQL environment
- dbt project setup
- dbt raw sources
- dbt staging models
- dbt staging tests
- Basic dbt marts
- Core dimension and fact tables
- Analytics marts for revenue, product performance, and delivery performance
- SQL data quality checks
- SQL interview practice queries
- README screenshots for ingestion, dbt staging, dbt marts, and SQL outputs

### In Progress

- Metabase dashboard integration
- More project documentation and interview notes

### Planned

- More custom dbt business tests
- dbt source freshness checks
- Metabase dashboard
- Airflow orchestration
- GitHub Actions CI/CD
- AWS-ready architecture notes
- SCD Type 2 snapshot
- Incremental models

---

## 3. Tech Stack

| Category         | Tools                   |
| ---------------- | ----------------------- |
| Language         | Python 3.11             |
| Database         | PostgreSQL              |
| Data Processing  | pandas                  |
| DB Access        | SQLAlchemy, psycopg2    |
| Transformation   | dbt Core, dbt-postgres  |
| Testing          | pytest, dbt tests       |
| Containerization | Docker Compose          |
| BI               | Metabase (planned)      |
| Orchestration    | Airflow (planned)       |
| CI/CD            | GitHub Actions (planned) |

---

## 4. Architecture

```text
CSV Retail Dataset
        ↓
Python ingestion pipeline
        ↓
Validation and column normalization
        ↓
PostgreSQL raw layer
        ↓
Ingestion metadata tracking
        ↓
dbt staging layer
        ↓
dbt marts layer
        ↓
Analytics-ready tables
        ↓
SQL analysis / Metabase dashboard (planned)
```

The current MVP focuses on a clear batch ELT flow:

```text
raw → staging → marts
```

Future versions will add orchestration, CI/CD, dashboarding, and AWS-ready design notes.

---

## 5. Project Structure

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
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── schema.yml
│       │   ├── stg_customers.sql
│       │   ├── stg_orders.sql
│       │   ├── stg_order_items.sql
│       │   ├── stg_products.sql
│       │   ├── stg_payments.sql
│       │   └── stg_shipments.sql
│       │
│       └── marts/
│           ├── core/
│           │   ├── dim_customers.sql
│           │   ├── dim_products.sql
│           │   ├── fact_orders.sql
│           │   ├── fact_order_items.sql
│           │   └── schema.yml
│           │
│           └── analytics/
│               ├── mart_daily_revenue.sql
│               ├── mart_product_performance.sql
│               ├── mart_delivery_performance.sql
│               └── schema.yml
│
├── sql_practice/
│   ├── 01_basic_select.sql
│   ├── 02_joins.sql
│   ├── 03_cte.sql
│   ├── 04_window_functions.sql
│   ├── 05_data_quality_checks.sql
│   └── 06_business_analysis.sql
│
├── screenshots/
├── docs/
├── docker-compose.yml
├── Makefile
├── requirements.txt
└── README.md
```

---

## 6. Data Ingestion Design

The ingestion layer loads raw retail CSV files into PostgreSQL raw tables.

### Current Features

* Config-driven ingestion using `TABLE_CONFIG`
* Multi-table loading support
* Input file existence validation
* Required column validation
* Primary key null validation
* Composite primary key validation for tables such as order items and payments
* Column name normalization
* Idempotent reload strategy using `TRUNCATE + INSERT`
* Ingestion metadata tracking
* Structured logging
* Basic unit testing with pytest

### Ingestion Flow

```text
validate input file exists
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
log success or failure
```

### Idempotency Strategy

The current MVP uses `TRUNCATE + INSERT` for raw table reloads. This keeps reruns deterministic and avoids duplicate appends when the same CSV file is loaded multiple times.

This strategy is simple and appropriate for a local batch MVP. For larger datasets, a future version should use incremental loading based on source update timestamps or ingestion watermarks.

---

## 7. Raw Tables

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

### Example Ingestion Metadata Query

```sql
SELECT *
FROM metadata.ingestion_runs
ORDER BY started_at DESC;
```

Tracked fields include:

* run_id
* source_name
* target_table
* row_count
* status
* started_at
* finished_at
* error_message

---

## 8. dbt Modeling Layers

This project uses dbt to transform raw retail data into clean and analytics-ready models.

### Staging Layer

Staging models clean and standardize raw data.

Current staging models:

```text
stg_customers
stg_orders
stg_order_items
stg_products
stg_payments
stg_shipments
```

Typical staging transformations:

* select useful columns
* rename columns consistently
* cast dates and timestamps
* cast numeric values
* normalize text fields
* prepare data for downstream marts

### Marts Layer

The marts layer contains business-ready dimension, fact, and analytics tables.

Core models:

```text
dim_customers
dim_products
fact_orders
fact_order_items
```

Analytics models:

```text
mart_daily_revenue
mart_product_performance
mart_delivery_performance
```

### Current Modeling Scope

The current MVP uses:

```text
raw → staging → marts
```

An intermediate layer is planned for future improvement when business logic becomes more complex.

---

## 9. Data Model

### Core Tables

| Model            | Type      | Grain                   | Purpose                    |
| ---------------- | --------- | ----------------------- | -------------------------- |
| dim_customers    | Dimension | One row per customer_id | Customer attributes        |
| dim_products     | Dimension | One row per product_id  | Product attributes         |
| fact_orders      | Fact      | One row per order_id    | Order-level metrics        |
| fact_order_items | Fact      | One row per order item  | Item-level revenue metrics |

### Analytics Marts

| Model                     | Grain                  | Key Metrics                                      |
| ------------------------- | ---------------------- | ------------------------------------------------ |
| mart_daily_revenue        | One row per order_date | total_orders, total_revenue, average_order_value |
| mart_product_performance  | One row per product_id | total_quantity, total_orders, total_revenue      |
| mart_delivery_performance | One row per order_id   | delivery_days, is_late_delivery                  |

### Revenue Logic

The current MVP calculates item-level gross revenue as:

```text
gross_revenue = item_price + freight_value
```

To avoid double counting revenue, order item metrics and payment metrics are aggregated at `order_id` grain before being joined into `fact_orders`.

---

## 10. Data Quality

Current data quality checks include Python-level validation, dbt tests, and manual SQL quality checks.

### Python Ingestion Validation

* required columns exist
* primary keys are not null
* composite primary keys are valid
* input files exist before loading
* ingestion status is tracked in metadata table

### dbt Tests

Current dbt tests include:

* not_null tests
* unique tests
* relationships tests
* accepted_values tests for selected fields

Example dbt test command:

```bash
cd dbt
dbt test --profiles-dir .
```

### Manual SQL Quality Checks

Manual SQL checks are stored in:

```text
sql_practice/05_data_quality_checks.sql
```

They cover:

- duplicate primary key checks
- null value checks
- orphan foreign key checks
- negative revenue and payment checks
- delivery date consistency checks
- mart output sanity checks

## 11. SQL Practice and Data Quality Checks

This project includes a dedicated SQL practice module covering analytical SQL patterns commonly used by Data Engineers and Data Analysts, along with data quality validation queries for the retail warehouse.

```text
sql_practice/
├── 01_basic_select.sql
├── 02_joins.sql
├── 03_cte.sql
├── 04_window_functions.sql
├── 05_data_quality_checks.sql
└── 06_business_analysis.sql
```

### Covered SQL Topics

#### Basic SQL
- Filtering and aggregation
- Grouping and sorting
- Revenue and order metrics

#### Joins
- Customer-to-order relationships
- Order-to-product relationships
- Payment and shipment enrichment
- Multi-table retail analytics queries

#### Common Table Expressions
- Revenue analysis
- Customer order summaries
- Delivery performance calculations
- Multi-step analytical transformations

#### Window Functions
- `ROW_NUMBER()`
- `RANK()`
- `LAG()`
- Running totals
- Revenue ranking by product and category
- Customer order sequencing

#### Data Quality Validation
- Raw table row count validation
- Duplicate primary key detection
- Null value checks
- Orphan foreign key checks
- Negative revenue and payment checks
- Referential integrity validation

#### Business Analytics
- Daily revenue trends
- Product performance analysis
- Order status distribution
- Customer purchasing behavior
- Delivery performance metrics
- Revenue by product category

These SQL exercises are designed to strengthen SQL fundamentals while validating warehouse data quality and supporting common business reporting use cases.

---

## 12. How to Run Locally

### 1. Start PostgreSQL

```bash
make up
```

### 2. Install dependencies

```bash
make install
```

### 3. Run ingestion pipeline

```bash
make load
```

### 4. Run Python tests

```bash
make test
```

### 5. Run dbt models

```bash
cd dbt
dbt debug --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

---

## 13. Validate Loaded Data

### Raw Table Counts

```sql
SELECT COUNT(*) FROM raw.raw_customers;
SELECT COUNT(*) FROM raw.raw_orders;
SELECT COUNT(*) FROM raw.raw_order_items;
SELECT COUNT(*) FROM raw.raw_products;
SELECT COUNT(*) FROM raw.raw_payments;
SELECT COUNT(*) FROM raw.raw_shipments;
```

### Ingestion Runs

```sql
SELECT *
FROM metadata.ingestion_runs
ORDER BY started_at DESC;
```

### Analytics Mart Checks

Depending on the dbt target schema, replace `marts` with your actual dbt schema if needed.

```sql
SELECT *
FROM marts.mart_daily_revenue
ORDER BY order_date
LIMIT 20;

SELECT *
FROM marts.mart_product_performance
ORDER BY total_revenue DESC
LIMIT 20;

SELECT *
FROM marts.mart_delivery_performance
ORDER BY delivery_days DESC NULLS LAST
LIMIT 20;
```

---

## 14. Analytics Outputs

### Daily Revenue Mart

`mart_daily_revenue` helps answer:

* How many orders were placed each day?
* What was the daily revenue?
* What was the average order value?

### Product Performance Mart

`mart_product_performance` helps answer:

* Which products generated the most revenue?
* Which product categories performed best?
* How many order items were sold per product?

### Delivery Performance Mart

`mart_delivery_performance` helps answer:

* Which orders were delivered late?
* How many days did delivery take?
* Which orders need delivery performance analysis?

---

## 15. Screenshots

### Pipeline Screenshots

### Raw Layer

#### Raw Table Counts
![Raw Table Counts](screenshots/raw_table_counts.png)

#### Ingestion Run History
![Ingestion Runs](screenshots/ingestion_runs.png)

---

### SQL Analytics & Data Quality

#### Data Quality Checks
Validation queries used to identify null values, duplicates, orphan records, and invalid business data.

![SQL Quality Checks](screenshots/sql_quality_checks.png)

#### Revenue Analysis by Day
Daily revenue aggregation query used to analyze sales performance trends.

![SQL Revenue by Day](screenshots/sql_revenue_by_day.png)

#### Top Products Analysis
SQL query used to identify best-selling products and product categories.

![SQL Top Products](screenshots/sql_top_products.png)

#### Customer Order Sequencing (Window Function)
ROW_NUMBER() window function used to determine purchase sequence and identify repeat customers.

![SQL Window Customer Order Number](screenshots/sql_window_customer_order_number.png)

---

### dbt Staging Layer

#### dbt Debug
![dbt Debug](screenshots/dbt_debug_pass.png)

#### dbt Run Staging
![dbt Run Staging](screenshots/dbt_run_staging_pass.png)

#### dbt Test Staging
![dbt Test Staging](screenshots/dbt_test_staging_pass.png)

#### Staging Table Counts
![Staging Table Counts](screenshots/staging_table_counts.png)

---

### dbt Marts Layer

#### dbt Run Marts
![dbt Run Marts](screenshots/dbt_run_marts_pass.png)

#### dbt Test Marts
![dbt Test Marts](screenshots/dbt_test_marts_pass.png)

#### Marts Table Counts
![Marts Table Counts](screenshots/marts_table_counts.png)

#### Daily Revenue Mart
![Mart Daily Revenue](screenshots/mart_daily_revenue.png)

#### Product Performance Mart
![Mart Product Performance](screenshots/mart_product_performance.png)

#### Delivery Performance Mart
![Mart Delivery Performance](screenshots/mart_delivery_performance.png)

---

## 16. Engineering Practices

This project currently implements:

* Config-driven ingestion
* Structured logging
* Validation before loading
* Idempotent reload strategy
* Metadata tracking
* Reproducible local PostgreSQL environment
* Python unit tests
* dbt transformation layers
* dbt model tests
* Basic dimensional modeling
* Analytics-ready marts
* Manual SQL data quality checks
* SQL interview practice queries

Planned engineering improvements:

* Convert important SQL quality checks into dbt singular tests
* Add more custom dbt business tests
* Add dbt source freshness checks
* SCD Type 2 snapshot
* Incremental models
* Airflow orchestration
* GitHub Actions CI/CD
* Metabase dashboard
* AWS-ready deployment notes

---

## 17. Interview Notes

### What does this project do?

This project builds a retail ELT warehouse that loads raw CSV data into PostgreSQL, validates data before loading, tracks ingestion runs, and uses dbt to transform raw data into clean staging models and analytics-ready marts for revenue, product, and delivery analysis.

### Why use dbt?

dbt helps separate SQL transformations into clear layers, manage model dependencies with `ref()`, document data models, and apply data tests such as not_null, unique, relationships, and accepted_values.

### Why create marts?

Marts are analytics-ready tables designed for business use cases. Instead of querying raw tables directly, marts provide clean and tested tables for reporting, dashboards, and analysis.

### What is the grain of fact_orders?

The grain of `fact_orders` is one row per `order_id`.

### How does the project avoid double counting revenue?

The project aggregates order item metrics and payment metrics by `order_id` before joining them into `fact_orders`. This prevents row multiplication when an order has multiple items or multiple payment records.

### What SQL topics does this project demonstrate?

The project includes SQL practice files for joins, CTEs, window functions, data quality checks, and business analysis queries such as daily revenue, product performance, delivery performance, and customer order sequencing.

### What are the current limitations?

The current MVP does not yet include Airflow orchestration, GitHub Actions CI/CD, Metabase dashboards, AWS deployment notes, SCD Type 2 snapshots, or incremental models. These are planned future improvements.

---

## 18. Roadmap

### Phase 1 - Ingestion Foundation

Completed:

- Python ingestion
- PostgreSQL raw layer
- Validation
- Logging
- Metadata tracking
- Idempotent loading
- Basic pytest coverage

### Phase 2 - Warehouse Modeling

Completed:

- dbt staging models
- dbt staging tests
- basic dbt marts
- dimension and fact tables
- analytics marts
- SQL practice queries
- SQL data quality checks
- README screenshots

### Phase 3 - Dashboard and Production Features

Planned:

- Metabase dashboard
- additional dbt business tests
- source freshness checks
- Airflow DAG
- GitHub Actions CI
- AWS-ready architecture notes
- SCD Type 2 snapshot
- incremental model

---

## 19. Future Improvements

- Add `dim_dates`
- Add `dim_sellers`
- Add customer retention mart
- Add intermediate dbt models
- Convert important SQL quality checks into dbt singular tests
- Add more custom business tests
- Add source freshness checks
- Add SCD Type 2 snapshot for product dimension
- Add incremental model for daily revenue
- Build Metabase dashboard
- Add Airflow orchestration
- Add GitHub Actions CI/CD
- Add AWS target architecture documentation

---

## 20. Author

Built as part of a Data Engineering portfolio project focused on production-style ELT workflows and analytics engineering.
