# Ghi chú tái tạo dashboard

Repository này không commit file Power BI dạng binary.
Thay vào đó, dự án cung cấp:

- các analytics mart trong PostgreSQL
- ví dụ SQL để tái tạo visual
- định nghĩa metric và hướng dẫn tái tạo dashboard

Thư mục `dashboards/screenshots/` hiện không chứa bộ screenshot dashboard đã chốt, nên phần dashboard trong repository được hỗ trợ chính thức bằng SQL và định nghĩa metric thay vì ảnh chụp màn hình.

Các nguồn dữ liệu gợi ý cho báo cáo:

- `analytics_marts.mart_daily_revenue`
- `analytics_marts.mart_product_performance`
- `analytics_marts.mart_delivery_performance`

Các visual gợi ý:

- xu hướng doanh thu theo ngày
- sản phẩm có doanh thu cao nhất
- tỷ lệ giao hàng trễ theo ngày đặt hàng

Xem [sql/analytics_queries.sql](sql/analytics_queries.sql) để dùng lại các câu truy vấn dashboard.
