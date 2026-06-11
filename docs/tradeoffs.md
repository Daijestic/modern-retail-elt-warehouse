# Trade-offs

## 1. Vì sao dùng PostgreSQL thay vì cloud warehouse

PostgreSQL được chọn vì dễ chạy local bằng Docker Compose, không tốn cloud cost và tích hợp tốt với dbt-postgres.
Với portfolio Intern/Fresher, PostgreSQL đủ để thể hiện SQL, warehouse layers, dbt modeling và data quality.

Trade-off là PostgreSQL không tối ưu cho analytical workload rất lớn như BigQuery, Snowflake hoặc Redshift. Project hiện phù hợp MVP local hơn là production cloud warehouse.

## 2. Vì sao dùng dbt thay vì viết toàn bộ SQL/Python thủ công

dbt giúp tổ chức transformation theo model, quản lý dependency bằng `ref()`, định nghĩa tests gần model và hỗ trợ source freshness.
Nếu viết toàn bộ SQL/Python thủ công, pipeline vẫn chạy được nhưng khó theo dõi lineage, khó test có hệ thống và khó mở rộng.

Trade-off là dbt cần setup profile/database connection và yêu cầu discipline trong cách đặt tên model, schema và tests.

## 3. Vì sao dùng `TRUNCATE + INSERT` cho MVP

Nguồn dữ liệu hiện là CSV batch nhỏ, nên `TRUNCATE + INSERT` giúp rerun ingestion mà không append duplicate rows.
Cách này đơn giản, deterministic và dễ debug khi đang xây dựng project portfolio.

Flow hiện tại:

```text
TRUNCATE raw table
-> INSERT latest CSV data
-> record metadata.ingestion_runs
```

## 4. Nhược điểm của `TRUNCATE + INSERT`

- Reload toàn bộ bảng dù chỉ thay đổi một phần nhỏ.
- Không giữ row-level history trong raw table.
- Không phù hợp cho dữ liệu lớn hoặc near-real-time ingestion.
- Nếu load fail sau khi truncate, cần dựa vào rerun/source file để khôi phục dữ liệu.
- Không phát hiện được thay đổi từng dòng như upsert/incremental strategy.

## 5. Khi nào nên chuyển sang incremental loading

Nên chuyển sang incremental loading khi dữ liệu lớn hơn, source có timestamp/update marker rõ ràng, pipeline cần chạy thường xuyên hoặc business cần giữ lịch sử thay đổi.

Các hướng nâng cấp:

- Upsert/merge theo primary key.
- Partition-based reload theo ngày.
- Raw history table có `ingested_at` hoặc `batch_id`.
- dbt incremental models cho analytics marts lớn.

## 6. Vì sao tách raw/staging/marts

- Raw layer giữ dữ liệu gần với nguồn để đối chiếu và debug.
- Staging layer chuẩn hóa kiểu dữ liệu, tên cột và khóa kỹ thuật.
- Marts layer chứa business logic, dimension/fact tables và bảng phục vụ analytics.

Cách tách layer giúp project dễ đọc, dễ test và dễ giải thích hơn so với query trực tiếp từ raw CSV tables.

## 7. Vì sao cần data quality checks nhiều lớp

Một lớp kiểm tra không đủ để bao phủ toàn bộ lỗi dữ liệu.
Python validation giúp chặn lỗi schema/key trước khi load.
dbt tests kiểm tra transformed models và relationships.
SQL checks giúp điều tra các rule business hoặc sanity checks chưa tự động hóa.

Cách tiếp cận nhiều lớp phù hợp với pipeline thực tế vì lỗi có thể xuất hiện ở source, ingestion, transformation hoặc aggregation.

## 8. Giới hạn hiện tại của project

- Chưa có Airflow orchestration.
- Chưa có GitHub Actions CI/CD.
- Chưa có dashboard BI hoàn chỉnh.
- Chưa có incremental models.
- Chưa có SCD Type 2.
- Chưa có cloud deployment.
- SQL data quality checks còn chạy thủ công.
- Data model chưa có `dim_dates`, `dim_sellers` hoặc customer retention mart.

## 9. Hướng nâng cấp production

Các nâng cấp hợp lý tiếp theo:

- Airflow DAG để schedule ingestion, dbt run/test và freshness check.
- GitHub Actions để chạy pytest và dbt checks khi mở pull request.
- BI dashboard bằng Power BI hoặc Metabase dựa trên analytics marts.
- Incremental loading và dbt incremental models cho dữ liệu lớn.
- SCD Type 2 cho dimension cần tracking lịch sử.
- AWS-ready architecture với S3, RDS/Redshift/Athena và secrets management.
- Convert SQL quality checks quan trọng thành dbt singular tests.

Những phần này hiện được xem là Future Improvements, chưa phải tính năng đã hoàn thành.
