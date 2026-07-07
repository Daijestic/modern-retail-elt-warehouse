# Đánh đổi

## Các đánh đổi đã chọn

### Dùng landing snapshot đã được kiểm soát thay vì raw history bất biến

Schema `raw` hiện là một full snapshot đã được kiểm soát.
Thiết kế này giúp dự án gọn hơn và dễ giải thích hơn, trong khi các bản ghi bị loại vẫn có thể audit lại trong `metadata.ingestion_rejections`.

### Dùng làm mới toàn bộ theo cách atomic thay vì incremental ingestion

Các file nguồn hiện là snapshot CSV đầy đủ và không có CDC marker.
Nạp qua bảng staging và thay thế dữ liệu trong transaction an toàn hơn so với giả định sai rằng dữ liệu chỉ append.

### Tách schema dbt theo target thay vì dùng chung staging/marts schema

Các lần chạy dbt ngoài production dùng `analytics_<custom_schema>` để tránh va chạm giữa local và CI.

### Dùng service-based CI thay vì chạy Docker Compose lồng trong CI

GitHub Actions dùng PostgreSQL service container cho các kiểm tra runtime và vẫn xác minh file Compose riêng.
Cách này giữ CI đơn giản hơn và thời gian chạy hợp lý hơn.

## Các phần cố ý chưa thêm

Repository này chủ động không thêm:

- Airflow
- Kafka
- Spark
- Kubernetes
- hạ tầng cloud
- secrets manager
- xử lý phân tán
- incremental model giả lập không phù hợp với ngữ nghĩa dữ liệu nguồn

## Ý nghĩa khi trình bày trong phỏng vấn

Các đánh đổi trên là những điểm đáng nói:

- bạn ưu tiên tính đúng đắn hơn là thêm độ phức tạp không cần thiết
- bạn làm cho lỗi có thể quan sát được thay vì che giấu chúng
- bạn giữ cho repository có thể clone và chạy lại được
- bạn phân biệt rõ giữa landing snapshot và raw history bất biến
