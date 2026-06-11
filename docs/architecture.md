# Architecture

## 1. Mục tiêu kiến trúc

Kiến trúc của Modern Retail ELT Warehouse được thiết kế để mô phỏng một batch ELT pipeline cho dữ liệu bán lẻ dạng CSV.
Mục tiêu là tách rõ ingestion, raw storage, transformation, data quality và analytics marts để dễ debug, dễ kiểm thử và dễ giải thích trong phỏng vấn.

Project ưu tiên sự đơn giản, reproducible local setup và tính đúng đắn của dữ liệu hơn là các thành phần production phức tạp như orchestration, CI/CD hoặc cloud deployment.

## 2. Data flow end-to-end

```text
data/raw/*.csv
-> ingestion/load_csv_to_postgres.py
-> raw tables in PostgreSQL
-> metadata.ingestion_runs
-> dbt staging models
-> dbt marts/core
-> dbt marts/analytics
-> dbt tests, source freshness, SQL checks
-> dashboard-ready analytics tables
```

## 3. Vai trò từng layer

### CSV/source

Các file CSV trong `data/raw/` là nguồn dữ liệu đầu vào. Project hiện load 6 nguồn: customers, orders, order_items, products, payments và shipments.

### Python ingestion

`ingestion/load_csv_to_postgres.py` đọc CSV bằng pandas, normalize tên cột, validate required columns và validate primary key/composite key không null.
Script cũng xử lý duplicate theo key và load dữ liệu vào PostgreSQL.

Ingestion được cấu hình bởi `ingestion/table_config.py`, giúp thêm hoặc tắt nguồn dữ liệu mà không cần sửa nhiều logic trong loader.

### PostgreSQL raw layer

Schema `raw` lưu dữ liệu sau ingestion ở dạng gần với nguồn. Các bảng raw được tạo trong `db/init.sql`, gồm:

- `raw.raw_customers`
- `raw.raw_orders`
- `raw.raw_order_items`
- `raw.raw_products`
- `raw.raw_payments`
- `raw.raw_shipments`

### `metadata.ingestion_runs`

Bảng `metadata.ingestion_runs` ghi lại mỗi lần chạy ingestion với `run_id`, source, target table, row count, thời gian bắt đầu/kết thúc, trạng thái và error message.
Đây là phần quan trọng để debug pipeline và kiểm tra lần load gần nhất.

### dbt staging

Staging models chuẩn hóa dữ liệu raw: cast ID về text, cast timestamp/numeric, lower/trim text fields, tạo composite key như `order_item_key` và `payment_key`.

### dbt marts/core

Core marts tạo dimension table và fact table:

- `dim_customers`
- `dim_products`
- `fact_orders`
- `fact_order_items`

Layer này gom business logic chính như order-level metrics, item-level revenue và delivery fields.

### dbt analytics marts

Analytics marts phục vụ reporting trực tiếp:

- `mart_daily_revenue`
- `mart_product_performance`
- `mart_delivery_performance`

### Data quality checks

Data quality được kiểm tra bằng 3 lớp: Python validation trước khi load, dbt tests/source freshness sau transformation và SQL checks.
Các SQL checks nằm trong `sql_practice/05_data_quality_checks.sql`.

## 4. Vì sao dùng ELT thay vì ETL

Project dùng ELT vì dữ liệu raw được đưa vào PostgreSQL trước, sau đó dbt xử lý transformation bằng SQL.
Cách này giúp giữ raw layer để đối chiếu, debug và viết lại transformation khi business logic thay đổi.

Với portfolio project, ELT cũng phù hợp vì dbt thể hiện rõ lineage, dependency và tests giữa các model.

## 5. Vì sao dùng PostgreSQL raw layer trước dbt

dbt cần một database/warehouse để chạy SQL transformation. PostgreSQL raw layer đóng vai trò nơi tập trung dữ liệu đã load từ CSV, giúp dbt source models có đầu vào ổn định.

PostgreSQL cũng dễ chạy local bằng Docker Compose, phù hợp để demo project mà không cần cloud cost.

## 6. Vì sao cần `metadata.ingestion_runs`

Nếu chỉ load CSV vào raw tables, rất khó biết lần chạy nào thành công, load bao nhiêu dòng hoặc lỗi ở nguồn nào.
`metadata.ingestion_runs` giúp theo dõi lịch sử pipeline ở mức vận hành.

Bảng này hỗ trợ trả lời các câu hỏi thực tế như: lần load gần nhất có thành công không, bảng nào bị lỗi, lỗi gì và row count có bất thường không.

## 7. Điểm production-like hiện có

- Config-driven ingestion.
- Validation trước khi load.
- Idempotent loading bằng `TRUNCATE + INSERT`.
- Metadata tracking cho pipeline runs.
- Structured logging.
- Docker Compose local database.
- dbt layering theo raw/staging/marts.
- dbt tests và source freshness.
- SQL data quality checks.
- pytest cho ingestion utilities.

## 8. Giới hạn hiện tại

- Pipeline đang chạy thủ công qua Makefile, chưa có Airflow orchestration.
- Chưa có CI/CD để tự động chạy pytest/dbt tests.
- Chưa có incremental loading hoặc SCD Type 2.
- PostgreSQL phù hợp local MVP nhưng không phải cloud analytical warehouse.
- SQL checks hiện là manual checks, chưa chuyển hết thành dbt singular tests.
- Dashboard BI hoàn chỉnh chưa được triển khai trong repo.

## 9. Future architecture

Nếu nâng cấp production hơn, kiến trúc có thể mở rộng theo hướng:

```text
CSV/S3
-> Airflow orchestration
-> Python ingestion
-> PostgreSQL/RDS or cloud warehouse
-> dbt run/test/freshness
-> CI/CD quality gate
-> BI dashboard
-> monitoring and alerts
```

Các hướng nâng cấp phù hợp gồm Airflow, GitHub Actions CI/CD, dashboard Power BI/Metabase, incremental models, SCD Type 2 và cloud-ready architecture trên AWS.
