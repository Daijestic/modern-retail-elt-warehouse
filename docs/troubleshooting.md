# Khắc phục sự cố

## PostgreSQL container không khởi động được

Trước tiên hãy kiểm tra file Compose:

```bash
python scripts/project_cli.py compose-config
```

Sau đó xem log:

```bash
python scripts/project_cli.py logs --follow
```

Nếu volume local đã cũ sau khi schema thay đổi:

```bash
python scripts/project_cli.py reset
python scripts/project_cli.py up
```

## dbt không kết nối được

Kiểm tra PostgreSQL có đang chạy và đã healthy hay chưa:

```bash
python scripts/project_cli.py wait-for-postgres
```

Đảm bảo `.env` hoặc biến môi trường có các giá trị:

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

## Ingestion thất bại do lỗi data contract

Hãy kiểm tra:

- log ở terminal với `error_type` và `error_message` cho lỗi cấp file
- `metadata.ingestion_runs` để xem trạng thái lần nạp bảng và lỗi cấp file
- `metadata.ingestion_rejections` để xem các dòng bị loại ở cấp dòng

Các nguyên nhân thường gặp:

- có cột không mong đợi
- va chạm tên cột sau chuẩn hóa
- giá trị phân loại không được hỗ trợ
- timestamp hoặc giá trị số không hợp lệ

## Cần khôi phục dữ liệu tốt trước đó

Loader hiện đã tự giữ lại dữ liệu tốt trước đó nếu refresh thất bại giữa chừng.
Nếu bạn muốn làm sạch hoàn toàn môi trường local:

```bash
python scripts/project_cli.py reset
python scripts/project_cli.py demo
```

## Cảnh báo về collation version

Trong một số môi trường Docker local, PostgreSQL có thể phát ra cảnh báo `collation version mismatch` khi volume cũ được tạo dưới một phiên bản thư viện khác.

Cách xử lý thường dùng:

1. Dừng và xóa volume local bằng `python scripts/project_cli.py reset`
2. Khởi động lại bằng `python scripts/project_cli.py demo`

Nếu cảnh báo vẫn còn trên máy của bạn, đây thường là vấn đề của PostgreSQL local hoặc runtime của container, không phải lỗi logic của pipeline.
