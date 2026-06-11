# Data Model

## 1. Mục tiêu data model

Data model của project biến dữ liệu bán lẻ raw thành các bảng dễ phân tích cho doanh thu, hiệu suất sản phẩm và giao hàng.
Mô hình được chia thành raw layer, staging layer, core marts và analytics marts để tách rõ dữ liệu nguồn, chuẩn hóa kỹ thuật và business metrics.

## 2. Raw layer

Raw tables được tạo trong `db/init.sql` và load bởi Python ingestion.

| Bảng | Grain | Key |
| --- | --- | --- |
| `raw.raw_customers` | Một dòng cho mỗi `customer_id` | `customer_id` |
| `raw.raw_orders` | Một dòng cho mỗi `order_id` | `order_id` |
| `raw.raw_order_items` | Một dòng cho mỗi item trong order | `order_id`, `order_item_id` |
| `raw.raw_products` | Một dòng cho mỗi `product_id` | `product_id` |
| `raw.raw_payments` | Một dòng cho mỗi payment sequence của order | `order_id`, `payment_sequential` |
| `raw.raw_shipments` | Một dòng cho mỗi `order_id` | `order_id` |

Raw layer giữ dữ liệu gần với nguồn CSV. Một số tên cột giữ nguyên lỗi chính tả từ dataset, ví dụ `product_name_lenght`; staging layer chuẩn hóa thành `product_name_length`.

Ngoài raw tables, project có `metadata.ingestion_runs` với grain là một dòng cho mỗi ingestion attempt.

## 3. Staging layer

Staging models nằm trong `dbt/models/staging/`.

| Model | Grain | Vai trò |
| --- | --- | --- |
| `stg_customers` | Một dòng cho mỗi customer | Chuẩn hóa thông tin khách hàng |
| `stg_orders` | Một dòng cho mỗi order | Chuẩn hóa order status và timestamp |
| `stg_order_items` | Một dòng cho mỗi order item | Tạo `order_item_key`, cast price/freight |
| `stg_products` | Một dòng cho mỗi product | Chuẩn hóa category và thuộc tính sản phẩm |
| `stg_payments` | Một dòng cho mỗi payment sequence | Tạo `payment_key`, chuẩn hóa payment type/value |
| `stg_shipments` | Một dòng cho mỗi order shipment | Chuẩn hóa ngày giao và ngày dự kiến |

Staging layer không cố gắng aggregate dữ liệu. Layer này chủ yếu làm sạch kỹ thuật để marts phía sau dễ đọc và dễ test.

## 4. Core marts

Core marts nằm trong `dbt/models/marts/core/`.

### Dimension tables

| Model | Grain | Phục vụ |
| --- | --- | --- |
| `dim_customers` | Một dòng cho mỗi `customer_id` | Phân tích theo khách hàng, city, state, zip code |
| `dim_products` | Một dòng cho mỗi `product_id` | Phân tích theo sản phẩm và product category |

Dimension table lưu thuộc tính mô tả đối tượng kinh doanh. Trong project này, dimension còn đơn giản và chưa có SCD Type 2.

### Fact tables

| Model | Grain | Metrics chính |
| --- | --- | --- |
| `fact_order_items` | Một dòng cho mỗi order item | `item_price`, `freight_value`, `gross_revenue` |
| `fact_orders` | Một dòng cho mỗi `order_id` | `item_count`, `total_order_value`, `total_payment_value`, `delivery_days`, `is_late_delivery` |

Fact table lưu sự kiện hoặc giao dịch có thể đo lường. `fact_order_items` dùng cho item-level revenue; `fact_orders` dùng cho order-level metrics và delivery analysis.

Logic chính:

```text
gross_revenue = item_price + freight_value
total_order_value = sum(gross_revenue) by order_id
delivery_days = delivered_customer_date - order_purchase_timestamp
is_late_delivery = delivered_customer_date > estimated_delivery_date
```

## 5. Analytics marts

Analytics marts nằm trong `dbt/models/marts/analytics/`.

| Model | Grain | Phục vụ |
| --- | --- | --- |
| `mart_daily_revenue` | Một dòng cho mỗi `order_date` | Theo dõi doanh thu, số đơn và average order value theo ngày |
| `mart_product_performance` | Một dòng cho mỗi `product_id` và `product_category_name` | Xem sản phẩm/category tạo doanh thu và số lượng bán |
| `mart_delivery_performance` | Một dòng cho mỗi `order_id` có thông tin giao hàng | Phân tích thời gian giao và đơn giao trễ |

## 6. Grain của các bảng quan trọng

- `stg_orders` và `fact_orders`: one row per `order_id`.
- `stg_order_items` và `fact_order_items`: one row per order item, định danh bằng `order_item_key`.
- `stg_payments`: one row per `payment_key`, tạo từ `order_id` và `payment_sequential`.
- `mart_daily_revenue`: one row per `order_date`.
- `mart_product_performance`: one row per product/category.
- `mart_delivery_performance`: one row per order.

## 7. Fact, dimension và mart phục vụ ai

- Dimension tables phục vụ analyst khi cần filter/group theo thuộc tính như customer state hoặc product category.
- Fact tables phục vụ phân tích giao dịch ở grain rõ ràng, tránh query trực tiếp raw tables.
- Analytics marts phục vụ dashboard hoặc báo cáo nhanh, nơi business user cần bảng đã aggregate và dễ đọc.

## 8. Cách tránh double counting

Trong `fact_orders`, item metrics từ `fact_order_items` được aggregate theo `order_id` trước khi join vào orders.
Payment metrics từ `stg_payments` cũng được aggregate theo `order_id` trước khi join.

Cách này tránh row multiplication khi một order có nhiều items hoặc nhiều payment records.

`mart_daily_revenue` lấy revenue từ `fact_orders`, tức là đã ở order grain, nên giảm rủi ro cộng trùng doanh thu theo item/payment join.

## 9. Assumptions và limitations

- Dataset hiện chưa có `dim_dates` hoặc `dim_sellers`.
- Product dimension chưa theo dõi lịch sử thay đổi.
- Các marts hiện là full-refresh dbt models, chưa có incremental model.
- `mart_product_performance` dùng `product_id` và `product_category_name`; dataset không có product name thân thiện cho business user.
- Một số SQL quality checks đang chạy thủ công, chưa tự động hóa thành quality gate.
