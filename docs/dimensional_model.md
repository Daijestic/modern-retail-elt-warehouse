# Mô hình dữ liệu

## Các tầng model

### Tầng landing

Được lưu trong `raw`.
Các bảng ở đây giữ lại hình dạng dữ liệu CSV đã được chấp nhận, cùng với các cột provenance để truy vết nguồn.

### Tầng staging

Được lưu trong `analytics_staging` ở target không phải production.
Trách nhiệm chính:

- đổi tên cột nhất quán
- ép kiểu dữ liệu một cách tường minh
- chuẩn hóa giá trị text
- đưa ra các cột lineage của ingestion

### Tầng core

Được lưu trong `analytics_marts` ở target không phải production.

Các model core:

- `dim_customers`
- `dim_products`
- `fact_orders`
- `fact_order_items`

### Tầng analytics

Cũng được lưu trong `analytics_marts`.

Các analytics marts:

- `mart_daily_revenue`
- `mart_product_performance`
- `mart_delivery_performance`

## Grain

### `dim_customers`

- một dòng cho mỗi `customer_id`

### `dim_products`

- một dòng cho mỗi `product_id`

### `fact_orders`

- một dòng cho mỗi `order_id`
- bao gồm các giá trị tổng hợp về item, thanh toán và giao hàng ở grain đơn hàng

### `fact_order_items`

- một dòng cho mỗi cặp `order_id` + `order_item_id`

### `mart_daily_revenue`

- một dòng cho mỗi `order_date`

### `mart_product_performance`

- một dòng cho mỗi `product_id`

### `mart_delivery_performance`

- một dòng cho mỗi `order_id`

## Logic doanh thu

- `fact_order_items.line_total_amount = item_price + freight_value`
- `fact_orders.total_order_value = tổng của fact_order_items.line_total_amount` theo từng đơn hàng
- `mart_daily_revenue.gross_order_value = tổng của fact_orders.total_order_value` cho các đơn không thuộc `canceled` và `unavailable`
- `mart_product_performance.total_item_revenue` và `mart_product_performance.total_freight_value` là hai thành phần tách riêng của `mart_product_performance.gross_order_value`

## Các ràng buộc được kiểm tra bằng test

- bảng dimension khách hàng và sản phẩm có business key duy nhất
- các bảng fact giữ đúng grain được thiết kế
- relationship giữa fact và dimension là hợp lệ
- các đơn đã giao có timestamp giao hàng
- tổng ở mart khớp với tổng ở fact
