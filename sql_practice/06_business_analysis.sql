-- Purpose: Business analysis queries based on analytics marts.

-- 1. Daily revenue from mart
SELECT
    order_date,
    total_orders,
    total_revenue,
    average_order_value
FROM marts.mart_daily_revenue
ORDER BY order_date desc
LIMIT 10;

-- 2. Top 20 products/categories by revenue
SELECT
    product_id,
    product_category_name,
    total_quantity,
    total_orders,
    total_revenue
FROM marts.mart_product_performance
ORDER BY total_revenue DESC
LIMIT 20;

-- 3. Delivery performance summary
SELECT
    COUNT(*) AS total_delivered_orders,
    SUM(late_delivery_flag) AS late_orders,
    ROUND(
        SUM(late_delivery_flag)::numeric / NULLIF(COUNT(*), 0) * 100,
        2
    ) AS late_delivery_rate_percent,
    AVG(delivery_days) AS average_delivery_days
FROM marts.mart_delivery_performance;

-- 4. Late delivery orders
SELECT
    order_id,
    customer_id,
    order_status,
    order_date,
    delivered_customer_date,
    estimated_delivery_date,
    delivery_days,
    is_late_delivery
FROM marts.mart_delivery_performance
WHERE is_late_delivery = TRUE
ORDER BY delivery_days DESC NULLS LAST
LIMIT 50;

-- 5. Monthly revenue trend
SELECT
    DATE_TRUNC('month', order_date)::date AS revenue_month,
    SUM(total_orders) AS total_orders,
    SUM(total_revenue) AS total_revenue,
    SUM(total_revenue) / NULLIF(SUM(total_orders), 0) AS average_order_value
FROM marts.mart_daily_revenue
GROUP BY DATE_TRUNC('month', order_date)::date
ORDER BY revenue_month;

-- 6. Product category revenue summary
SELECT
    product_category_name,
    SUM(total_quantity) AS total_quantity,
    SUM(total_orders) AS total_orders,
    SUM(total_revenue) AS total_revenue
FROM marts.mart_product_performance
GROUP BY product_category_name
ORDER BY total_revenue DESC
LIMIT 10;