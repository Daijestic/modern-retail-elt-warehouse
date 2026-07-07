# Chất lượng dữ liệu

## Các lớp kiểm soát chất lượng

Dự án áp dụng kiểm soát chất lượng dữ liệu ở ba lớp:

1. kiểm tra hợp lệ bằng Python trước khi dữ liệu được nạp vào PostgreSQL
2. ghi metadata và lưu vết bản ghi bị loại trong PostgreSQL trong lúc ingestion
3. test ở source, model và business rule trong dbt sau bước transform

## Quy tắc kiểm tra hợp lệ ở ingestion

Kiểm tra ở mức file:

- file tồn tại
- file không rỗng
- file có thể giải mã bằng UTF-8
- có đầy đủ cột bắt buộc
- tên cột sau chuẩn hóa không bị va chạm
- không có cột ngoài mong đợi sau khi chuẩn hóa

Kiểm tra ở mức từng dòng:

- cột khóa chính không được null hoặc rỗng
- giá trị khóa chính không được trùng trong cùng file
- các cột số được cấu hình phải ép kiểu được
- các cột timestamp được cấu hình phải ép kiểu được
- các giá trị phân loại được cấu hình phải thuộc tập hỗ trợ
- các cột giá tiền được cấu hình không được âm

## Chính sách xử lý lỗi và bản ghi bị loại

Lỗi cấp file và lỗi cấp dòng được lưu ở hai nơi khác nhau:

- lỗi cấp file làm bảng đó thất bại và được ghi vào `metadata.ingestion_runs.error_type` cùng `metadata.ingestion_runs.error_message`
- lỗi cấp dòng được ghi từng dòng vào `metadata.ingestion_rejections`

Các `error_type` cấp file phổ biến:

- `MISSING_REQUIRED_COLUMN`
- `UNEXPECTED_COLUMN`
- `DUPLICATED_COLUMN_NAME`
- `EMPTY_FILE`
- `ENCODING_ERROR`
- `SCHEMA_DRIFT`
- `NO_VALID_ROWS`
- `FileNotFoundError`

Các `reason_code` cấp dòng phổ biến trong `metadata.ingestion_rejections`:

- `DUPLICATE_PRIMARY_KEY`
- `NULL_PRIMARY_KEY`
- `INVALID_TIMESTAMP`
- `INVALID_NUMERIC_VALUE`
- `NEGATIVE_PRICE`
- `UNSUPPORTED_STATUS`

Chính sách chung:

- lỗi ở mức data contract của file sẽ làm bảng đó thất bại
- lỗi dữ liệu ở mức từng dòng sẽ được cách ly
- các dòng bị loại không được nạp vào `raw`
- dbt chỉ đọc các dòng hợp lệ đã được nạp vào tầng landing

## dbt tests

dbt hiện bổ sung:

- test `not_null` và `unique` ở source trong những trường hợp phù hợp
- test `not_null`, `unique`, `relationships` và `accepted_values` ở tầng staging
- kiểm tra relationship và grain ở tầng core
- kiểm tra business rule để đối soát doanh thu không âm và tính nhất quán giữa mart với fact

## Vì sao bản ghi bị loại tách khỏi landing snapshot

Schema `raw` được xem là một landing snapshot đã được kiểm soát, không phải bản sao byte-for-byte hoàn hảo của file gốc.
Các bản ghi bị loại vẫn có thể audit lại, nhưng được tách khỏi dữ liệu landing đáng tin cậy để các model dbt ở phía sau không phải tự lọc thủ công dữ liệu lỗi.
