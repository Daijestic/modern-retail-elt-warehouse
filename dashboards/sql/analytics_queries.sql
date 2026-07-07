-- Xu hướng doanh thu theo ngày
select
    order_date,
    total_orders,
    gross_order_value,
    average_gross_order_value
from analytics_marts.mart_daily_revenue
order by order_date;

-- Bảng xếp hạng sản phẩm theo doanh thu
select
    product_id,
    product_category_name,
    total_quantity,
    total_orders,
    total_item_revenue,
    total_freight_value,
    gross_order_value
from analytics_marts.mart_product_performance
order by gross_order_value desc
limit 20;

-- Hiệu quả giao hàng
select
    order_date,
    count(*) as delivered_orders,
    avg(late_delivery_flag) as late_delivery_rate,
    avg(delivery_days) as avg_delivery_days
from analytics_marts.mart_delivery_performance
group by order_date
order by order_date;
