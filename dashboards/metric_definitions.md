# Định nghĩa metric cho dashboard

- `gross_order_value`: `mart_daily_revenue.gross_order_value`, tương đương tổng `fact_orders.total_order_value` cho các đơn không thuộc `canceled` và `unavailable`
- `average_gross_order_value`: `mart_daily_revenue.gross_order_value / mart_daily_revenue.total_orders`
- `total_item_revenue`: tổng của `mart_product_performance.total_item_revenue`
- `total_freight_value`: tổng của `mart_product_performance.total_freight_value`
- `late_delivery_flag`: nhận giá trị `1` khi ngày giao thực tế muộn hơn ngày giao dự kiến, ngược lại là `0`
- `late_delivery_rate`: giá trị trung bình của `late_delivery_flag`
- `total_quantity`: số dòng order item theo từng sản phẩm
