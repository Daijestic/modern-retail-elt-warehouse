# Hợp đồng dữ liệu

## Cách áp dụng data contract

Tầng ingestion dùng cấu hình tường minh cho từng bảng thay vì suy luận schema tự động.
Mỗi file nguồn được cấu hình phải khớp với đúng tên file kỳ vọng và tập cột sau khi chuẩn hóa.

Quy tắc chuẩn hóa tên cột:

- loại bỏ khoảng trắng ở đầu và cuối
- chuyển về chữ thường
- thay các chuỗi không phải ký tự chữ hoặc số bằng `_`
- gộp nhiều dấu gạch dưới liên tiếp thành một dấu `_`
- loại bỏ dấu gạch dưới ở đầu và cuối

Ví dụ:

- `Order Status` -> `order_status`
- `order-status` -> `order_status`

Nếu hai cột nguồn khác nhau cùng chuẩn hóa về một tên, bảng đó sẽ thất bại với `DUPLICATED_COLUMN_NAME`.

## Các file được kỳ vọng

### `customers.csv`

Cột bắt buộc:

- `customer_id`
- `customer_unique_id`
- `customer_zip_code_prefix`
- `customer_city`
- `customer_state`

### `orders.csv`

Cột bắt buộc:

- `order_id`
- `customer_id`
- `order_status`
- `order_purchase_timestamp`

Cột tùy chọn:

- `order_approved_at`
- `order_delivered_customer_date`
- `order_estimated_delivery_date`

Giá trị `order_status` được chấp nhận:

- `approved`
- `canceled`
- `created`
- `delivered`
- `invoiced`
- `processing`
- `shipped`
- `unavailable`

### `order_items.csv`

Cột bắt buộc:

- `order_id`
- `order_item_id`
- `product_id`
- `seller_id`
- `price`
- `freight_value`

Cột tùy chọn:

- `shipping_limit_date`

### `products.csv`

Cột bắt buộc:

- `product_id`
- `product_category_name`

Cột tùy chọn:

- `product_name_lenght`
- `product_description_lenght`
- `product_photos_qty`
- `product_weight_g`
- `product_length_cm`
- `product_height_cm`
- `product_width_cm`

Ghi chú:

- `product_name_lenght` và `product_description_lenght` là tên cột bị viết sai từ schema nguồn
- tầng landing giữ nguyên tên nguồn để trung thực với file CSV
- tầng staging chuẩn hóa thành `product_name_length` và `product_description_length`

### `payments.csv`

Cột bắt buộc:

- `order_id`
- `payment_sequential`
- `payment_type`
- `payment_value`

Cột tùy chọn:

- `payment_installments`

Giá trị `payment_type` được chấp nhận:

- `bank_transfer`
- `cash_on_delivery`
- `credit_card`
- `debit_card`
- `e_wallet`
- `voucher`

### `shipments.csv`

Cột bắt buộc:

- `order_id`
- `delivered_customer_date`
- `estimated_delivery_date`

## Điều kiện thất bại ở mức file

Các điều kiện sau sẽ làm bảng đó thất bại, giữ nguyên snapshot tốt gần nhất, và được ghi vào `metadata.ingestion_runs.error_type` cùng `metadata.ingestion_runs.error_message`:

- thiếu file
- file rỗng
- lỗi encoding
- thiếu cột bắt buộc
- có cột không mong đợi
- schema drift
- va chạm tên cột sau chuẩn hóa
- tất cả các dòng đều bị loại

## Điều kiện loại bản ghi ở mức dòng

Các điều kiện sau sẽ đưa dòng lỗi vào vùng cách ly, trong khi phần dữ liệu hợp lệ còn lại vẫn được nạp:

- khóa chính rỗng
- khóa chính trùng lặp
- giá trị số không hợp lệ
- timestamp không hợp lệ
- giá trị phân loại không được hỗ trợ
- giá âm ở cột giá tiền

Các bản ghi bị loại được ghi vào `metadata.ingestion_rejections` và không đi vào các model dbt ở phía sau.
Nếu mọi dòng trong file đều bị loại, bảng sẽ `FAILED` với `error_type = NO_VALID_ROWS`, nhưng chi tiết từng dòng vẫn nằm trong `metadata.ingestion_rejections`.
