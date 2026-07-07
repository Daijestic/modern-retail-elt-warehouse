# Thiết kế ingestion

## Mục tiêu

Pipeline ingestion được thiết kế để phục vụ:

- phát triển local có tính xác định
- kiểm tra hợp lệ tường minh và lưu vết bản ghi bị loại
- lan truyền lỗi rõ ràng
- nạp qua bảng staging và thay thế dữ liệu trong transaction
- metadata có thể tái lập để hỗ trợ debug

## Điểm vào chạy lệnh

- `python -m ingestion`
- `python -m ingestion.load_csv_to_postgres`
- `python scripts/project_cli.py load`

Mỗi điểm vào sẽ trả về exit code `0` cho `SUCCESS` hoặc `PARTIAL_SUCCESS`, và exit code khác `0` cho `FAILED`.

## Trình tự nạp dữ liệu

Với mỗi bảng nguồn được cấu hình:

1. Kiểm tra file có tồn tại hay không.
2. Đọc CSV dưới dạng chuỗi để giữ nguyên giá trị nguồn trước khi ép kiểu ở staging.
3. Chuẩn hóa tên cột và so sánh với data contract đã cấu hình.
4. Chạy các quy tắc kiểm tra hợp lệ có thể tái sử dụng.
5. Tách dữ liệu thành hai nhóm: dòng hợp lệ và dòng bị loại.
6. Thêm các cột provenance cho tầng landing:
   - `_ingestion_run_id`
   - `_source_file`
   - `_source_row_number`
   - `_ingested_at`
   - `_file_checksum`
   - `_source_modified_at`
7. Nạp các dòng hợp lệ vào một bảng staging tạm trong PostgreSQL.
8. Kiểm tra số dòng của bảng staging.
9. Trong transaction, khóa bảng đích, `TRUNCATE`, rồi nạp lại dữ liệu từ bảng staging.
10. Xóa bảng staging tạm.
11. Ghi metadata ở mức bảng vào `metadata.ingestion_runs` và các dòng bị loại vào `metadata.ingestion_rejections`.

## Nạp qua bảng staging và thay thế dữ liệu trong transaction

Dự án không ghi trực tiếp từ CSV vào bảng landing đích.
Thay vào đó, nó dùng cơ chế nạp qua bảng staging và thay thế dữ liệu trong transaction:

- các dòng hợp lệ được nạp vào `raw.<table>__stg_<id>`
- số dòng ở staging được kiểm tra trước khi thay thế
- việc thay thế dữ liệu bảng đích diễn ra trong một transaction duy nhất bằng `LOCK TABLE`, `TRUNCATE` rồi `INSERT ... SELECT`
- nếu lỗi xảy ra trước hoặc trong lúc thay thế, transaction sẽ rollback và giữ nguyên dữ liệu tốt trước đó

Thiết kế này giữ dự án đơn giản nhưng tránh được tình huống bảng đích bị rỗng do refresh thất bại.

## Phân biệt lỗi cấp file và lỗi cấp dòng

- lỗi cấp file làm bảng đó `FAILED` và được ghi vào `metadata.ingestion_runs.error_type` cùng `metadata.ingestion_runs.error_message`
- lỗi cấp dòng chỉ tạo bản ghi trong `metadata.ingestion_rejections` và làm tăng `rows_rejected`
- nếu mọi dòng đều bị loại, bảng vẫn `FAILED` với `error_type = NO_VALID_ROWS`, còn chi tiết từng dòng bị loại vẫn nằm trong `metadata.ingestion_rejections`

## Ghi log

Logger của ingestion ghi các trường ngữ cảnh cho từng lần nạp bảng, bao gồm:

- `run_id`
- `table_run_id`
- `source_file`
- `target_table`
- `status`
- `rows_read`
- `rows_loaded`
- `rows_rejected`
- `duplicate_count`
- `duration_seconds`
- `error_type`
- `error_message`

## Trạng thái pipeline

Trạng thái ở mức từng bảng:

- `SUCCESS`: không có lỗi bảng và không có dòng bị loại
- `PARTIAL_SUCCESS`: bảng được nạp thành công nhưng có ít nhất một dòng bị loại
- `FAILED`: bảng không được nạp hoàn tất

Trạng thái toàn pipeline:

- `SUCCESS`: tất cả các bảng được bật đều thành công và không có bản ghi bị loại
- `PARTIAL_SUCCESS`: tất cả các bảng được bật đều được nạp, nhưng có ít nhất một bảng có bản ghi bị loại
- `FAILED`: có ít nhất một bảng được bật bị lỗi

## Vì sao chưa dùng incremental ingestion

Tầng landing hiện là một full snapshot đã được kiểm soát.
Do các file nguồn không cung cấp CDC hoặc delete marker, cách an toàn nhất hiện tại là làm mới toàn bộ theo cách atomic kết hợp với metadata lineage.
Cách này đơn giản hơn, đúng hơn và dễ giải thích hơn so với một logic incremental giả lập dễ bỏ sót delete.
