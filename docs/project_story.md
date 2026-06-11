# Project Story

## 1. Tóm tắt ngắn

Modern Retail ELT Warehouse là project portfolio mô phỏng một batch ELT pipeline cho dữ liệu bán lẻ.
Project load CSV vào PostgreSQL raw layer, ghi nhận ingestion metadata, transform bằng dbt và tạo analytics marts cho doanh thu, sản phẩm và giao hàng.

Project được xây dựng để thể hiện kỹ năng Data Engineering nền tảng: ingestion, validation, warehouse modeling, data quality và documentation.

## 2. Bài toán kinh doanh

Dữ liệu bán lẻ dạng CSV thường khó dùng trực tiếp cho phân tích vì thiếu chuẩn hóa, có khả năng trùng khóa, thiếu cột quan trọng hoặc quan hệ giữa bảng chưa được kiểm tra.
Business cần các bảng sạch hơn để trả lời câu hỏi về doanh thu theo ngày, sản phẩm bán tốt và hiệu quả giao hàng.

## 3. Pipeline end-to-end

```text
CSV retail data
-> Python ingestion
-> PostgreSQL raw layer
-> metadata.ingestion_runs
-> dbt staging models
-> dbt core marts
-> dbt analytics marts
-> dbt tests and SQL checks
-> analytics outputs
```

## 4. Những phần đã triển khai

- Config-driven CSV ingestion bằng Python.
- Required column validation.
- Primary key và composite primary key null validation.
- Column name normalization.
- Duplicate handling theo primary key, giữ bản ghi cuối cùng.
- Idempotent reload bằng `TRUNCATE + INSERT`.
- Ingestion run tracking với row count, status, timestamps và error message.
- dbt staging models cho 6 raw sources.
- Core marts gồm dimension và fact tables.
- Analytics marts cho daily revenue, product performance và delivery performance.
- dbt tests, source freshness và SQL data quality checks.
- pytest cho validators và table config.
- Documentation và screenshots kết quả chạy.

## 5. Các quyết định kỹ thuật chính

### Vì sao dùng Python ingestion?

Python phù hợp để đọc CSV, validate schema, xử lý lỗi, log trạng thái và load dữ liệu vào PostgreSQL bằng pandas/SQLAlchemy.

### Vì sao dùng PostgreSQL?

PostgreSQL dễ chạy local bằng Docker, hỗ trợ SQL tốt và tích hợp với dbt-postgres. Đây là lựa chọn hợp lý cho MVP portfolio trước khi mở rộng sang cloud warehouse.

### Vì sao dùng dbt?

dbt giúp chia transformation thành model rõ ràng, quản lý dependency bằng `ref()`, định nghĩa tests và thể hiện data lineage tốt hơn so với SQL scripts rời rạc.

### Vì sao tách raw, staging và marts?

Raw giữ dữ liệu gần nguồn. Staging chuẩn hóa kỹ thuật. Marts chứa business logic và bảng phục vụ analytics. Cách tách này giúp dễ debug, dễ test và dễ mở rộng.

## 6. Data quality strategy

Project kiểm soát data quality ở 3 lớp:

- Ingestion validation: file tồn tại, required columns, primary key/composite key không null.
- dbt tests: `not_null`, `unique`, `relationships`, `accepted_values` và source freshness.
- SQL checks: duplicate, null, orphan records, negative values và logic checks.

## 7. Giới hạn hiện tại

Project chưa có Airflow orchestration, GitHub Actions CI/CD, dashboard BI hoàn chỉnh, incremental models, SCD Type 2 hoặc AWS deployment.
Các phần này được đặt trong Future Improvements để tránh overclaim.

## 8. Interview pitch 60 giây

Modern Retail ELT Warehouse là project ELT cho dữ liệu bán lẻ dạng CSV.
Em dùng Python để ingest dữ liệu vào PostgreSQL raw layer, có validation required columns, primary key/composite key, duplicate handling và logging.
Pipeline cũng có metadata tracking trong `metadata.ingestion_runs`.
Sau đó em dùng dbt để xây staging models, dimension/fact tables và analytics marts như `mart_daily_revenue`, `mart_product_performance`, `mart_delivery_performance`.
Project cũng có dbt tests, source freshness, SQL data quality checks và pytest để kiểm soát chất lượng dữ liệu ở nhiều lớp.
