# Data Quality

## 1. Vì sao data quality quan trọng

Data warehouse chỉ hữu ích khi dữ liệu đủ tin cậy để phân tích.
Với dữ liệu bán lẻ, lỗi như thiếu khóa, duplicate orders, orphan records hoặc doanh thu âm có thể làm sai báo cáo doanh thu, sản phẩm và giao hàng.

Project này kiểm soát data quality ở nhiều lớp để lỗi được phát hiện sớm và dễ debug.

```text
Python ingestion validation
-> dbt tests and source freshness
-> SQL quality checks
```

## 2. Data quality ở ingestion layer

Ingestion layer nằm trong `ingestion/load_csv_to_postgres.py` và `ingestion/validators.py`.

Các rule hiện có:

- Kiểm tra source file tồn tại trước khi đọc CSV.
- Normalize column names bằng cách trim và chuyển lowercase.
- Kiểm tra required columns theo `ingestion/table_config.py`.
- Kiểm tra primary key hoặc composite key tồn tại trong DataFrame.
- Kiểm tra primary key/composite key không null.
- Ép key columns sang string trước khi xử lý duplicate.
- Nếu duplicate theo key, ghi warning và giữ bản ghi cuối cùng.
- Load idempotent bằng `TRUNCATE TABLE raw.<table>` rồi insert dữ liệu mới.
- Ghi kết quả chạy vào `metadata.ingestion_runs`.
- Khi lỗi xảy ra, log exception và lưu `status = FAILED` cùng `error_message`.

Primary key/composite key hiện được cấu hình:

| Source | Key |
| --- | --- |
| customers | `customer_id` |
| orders | `order_id` |
| order_items | `order_id`, `order_item_id` |
| products | `product_id` |
| payments | `order_id`, `payment_sequential` |
| shipments | `order_id` |

## 3. Data quality ở dbt layer

dbt tests được định nghĩa trong các file `schema.yml` của staging, core marts và analytics marts.

Các loại test hiện có:

- `not_null`: kiểm tra cột bắt buộc không null.
- `unique`: kiểm tra key hoặc mart grain không bị trùng.
- `relationships`: kiểm tra foreign key giữa staging/fact/dimension.
- `accepted_values`: kiểm tra `order_status` nằm trong danh sách hợp lệ.

Project cũng có source freshness trong `dbt/models/sources.yml`, dùng `ingested_at` với ngưỡng cảnh báo 24 giờ và lỗi 48 giờ.

Command liên quan:

```bash
make dbt-test
make dbt-freshness
```

## 4. SQL quality checks

Manual SQL checks nằm trong `sql_practice/05_data_quality_checks.sql`.

Nhóm check hiện có:

- Null check: ví dụ `customer_id` trong orders, `order_id` trong order_items.
- Duplicate check: customers, orders, products.
- Orphan records: orders không có customer, order_items không có order hoặc product.
- Negative value checks: price, freight value, payment value.
- Logic checks: delivered date trước purchase date, delivery days âm.
- Mart sanity checks: revenue âm trong `mart_daily_revenue` và `mart_product_performance`.

Chạy SQL checks:

```bash
make run-sql FILE=sql_practice/05_data_quality_checks.sql
```

Nếu dbt target schema khác `marts`, cần chỉnh schema trong SQL checks cho phù hợp trước khi chạy.

## 5. Khi test fail thì xử lý thế nào

1. Xác định fail ở layer nào: ingestion, dbt test, source freshness hay SQL check.
2. Nếu ingestion fail, xem log và bảng `metadata.ingestion_runs` để biết source, target table và error message.
3. Nếu dbt test fail, kiểm tra model liên quan và query dữ liệu lỗi bằng SQL.
4. Nếu source freshness fail, kiểm tra ingestion gần nhất và giá trị `ingested_at` trong raw tables.
5. Nếu SQL check trả về rows, coi đó là các record cần điều tra thay vì bỏ qua.
6. Sau khi sửa dữ liệu hoặc logic, chạy lại ingestion/dbt/test tương ứng để xác nhận.

## 6. Quality rule nên bổ sung trong tương lai

- Custom dbt singular tests cho negative revenue và delivery logic.
- Test đối chiếu `total_order_value` với tổng order items.
- Test đối chiếu `total_payment_value` với order value khi business rule cho phép.
- Test ngày đặt hàng không nằm trong tương lai.
- Test delivered orders phải có delivered date.
- CI/CD quality gate chạy pytest, dbt compile, dbt test và source freshness.
- Data quality dashboard hoặc alert khi pipeline fail.
