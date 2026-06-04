-- Purpose: Practice CTEs for readable multi-step business queries.

-- 1. Daily revenue using CTE
WITH order_revenue AS (
    SELECT
        o.order_id,
        CAST(o.order_purchase_timestamp AS DATE) AS order_date,
        SUM(oi.price + oi.freight_value) AS order_revenue
    FROM raw.raw_orders AS o
    JOIN raw.raw_order_items AS oi
        ON o.order_id = oi.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY
        o.order_id,
        CAST(o.order_purchase_timestamp AS DATE)
)
SELECT
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(order_revenue) AS total_revenue,
    SUM(order_revenue) / COUNT(DISTINCT order_id) AS average_order_value
FROM order_revenue
GROUP BY order_date
ORDER BY order_date;

-- 2. Product category revenue
WITH product_revenue AS (
    SELECT
        p.product_category_name,
        oi.order_id,
        oi.price + oi.freight_value AS gross_revenue
    FROM raw.raw_order_items AS oi
    LEFT JOIN raw.raw_products AS p
        ON oi.product_id = p.product_id
)
SELECT
    product_category_name,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(gross_revenue) AS total_revenue
FROM product_revenue
GROUP BY product_category_name
ORDER BY total_revenue DESC;

-- 3. Payment revenue by payment type
WITH payment_summary AS (
    SELECT
        payment_type,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(payment_value) AS total_payment_value
    FROM raw.raw_payments
    GROUP BY payment_type
)
SELECT
    payment_type,
    total_orders,
    total_payment_value
FROM payment_summary
ORDER BY total_payment_value DESC;

-- 4. Delivery performance by order
WITH delivery_base AS (
    SELECT
        o.order_id,
        o.order_status,
        CAST(o.order_purchase_timestamp AS DATE) AS order_date,
        CAST(s.delivered_customer_date AS DATE) AS delivered_date,
        CAST(s.estimated_delivery_date AS DATE) AS estimated_delivery_date
    FROM raw.raw_orders AS o
    LEFT JOIN raw.raw_shipments AS s
        ON o.order_id = s.order_id
)
SELECT
    order_id,
    order_status,
    order_date,
    delivered_date,
    estimated_delivery_date,
    delivered_date - order_date AS delivery_days,
    CASE
        WHEN delivered_date > estimated_delivery_date THEN TRUE
        ELSE FALSE
    END AS is_late_delivery
FROM delivery_base
WHERE delivered_date IS NOT NULL;

-- 5. Customer order summary
WITH customer_orders AS (
    SELECT
        c.customer_id,
        c.customer_state,
        o.order_id
    FROM raw.raw_customers AS c
    LEFT JOIN raw.raw_orders AS o
        ON c.customer_id = o.customer_id
)
SELECT
    customer_state,
    COUNT(DISTINCT customer_id) AS total_customers,
    COUNT(DISTINCT order_id) AS total_orders
FROM customer_orders
GROUP BY customer_state
ORDER BY total_orders DESC;