# Nhật ký thay đổi

## [1.0.0] - 2026-07-07

### Đã thêm

- kết quả kiểm tra hợp lệ ở ingestion có cấu trúc và cơ chế lưu vết bản ghi bị loại
- cơ chế làm mới toàn bộ theo cách atomic bằng stage-and-swap
- metadata ở mức pipeline run và table load
- Python task runner chạy đa nền tảng cho demo và quy trình CI local
- kiểm thử tích hợp cho ingestion với PostgreSQL thật
- các test đối soát và business rule trong dbt
- workflow CI bằng GitHub Actions
- bộ tài liệu về kiến trúc, ingestion, metadata, kiểm thử và khắc phục sự cố

### Đã thay đổi

- thiết kế lại các bảng landing để lưu giá trị nguồn đã được chấp nhận cùng các cột provenance
- thay cơ chế nuốt lỗi kiểu prototype bằng lan truyền lỗi tường minh
- chốt phiên bản dependency để cài đặt có thể tái lập
- cập nhật Docker Compose sang cấu hình dựa trên biến môi trường và health check
- cập nhật chiến lược đặt tên schema dbt để tách biệt môi trường ngoài production

### Đã loại bỏ

- các test ingestion cũ bị trùng trong `ingestion/tests/`
- các thư mục dashboard placeholder rỗng, không có tài liệu đi kèm
- các file tài liệu cũ không còn khớp với cách triển khai hiện tại
