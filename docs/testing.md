# Kiểm thử

## Cấu trúc thư mục test

```text
tests/
  unit/
  integration/
```

## Kiểm thử đơn vị

Kiểm thử đơn vị tập trung vào các phần logic có tính xác định và không cần database đang chạy:

- chuẩn hóa tên cột
- phát hiện va chạm tên cột sau chuẩn hóa
- tính checksum
- kết quả kiểm tra hợp lệ có cấu trúc
- tính đúng đắn của cấu hình bảng
- tính toán trạng thái pipeline

Chạy:

```bash
python -m pytest tests/unit -q
```

## Kiểm thử tích hợp

Kiểm thử tích hợp dùng một PostgreSQL thật để xác minh:

- ingestion end-to-end
- ghi metadata
- lưu vết bản ghi bị loại
- chạy lại không làm sai dữ liệu
- lỗi trước khi thay thế bảng đích
- lỗi trong lúc thay thế bảng đích
- bảo toàn dữ liệu tốt trước đó
- đầu ra `dbt build`

Chạy:

```bash
python -m pytest tests/integration -q
```

Trong môi trường local, nếu PostgreSQL hoặc dbt chưa sẵn sàng, các test phụ thuộc database có thể tự động được skip để không chặn các vòng phát triển chỉ tập trung vào unit test.

## Mô phỏng CI trong local

Dùng:

```bash
python scripts/project_cli.py ci-local
```

Lệnh này chạy:

- compile check
- kiểm tra cấu hình Docker Compose
- `pytest`
- `dbt deps`
- `dbt parse`
- `dbt build`
- các truy vấn xác minh

## Workflow CI

GitHub Actions hiện chạy:

- cài dependency
- kiểm tra `docker compose config`
- compile check cho Python
- kiểm thử đơn vị
- kiểm thử tích hợp cho ingestion
- `dbt parse`
- kiểm thử tích hợp có dùng dbt
- sinh dbt docs
- các truy vấn xác minh đầu ra

Trong CI, PostgreSQL và dbt là dependency bắt buộc.
Các fixture test sẽ cho phép skip trong local, nhưng sẽ fail thay vì skip khi chạy với `CI=true`, nên workflow không thể thành công nếu toàn bộ integration test bị bỏ qua ngoài dự kiến vì thiếu PostgreSQL hoặc dbt.
