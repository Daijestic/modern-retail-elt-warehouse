
-- 1. Orders with customer location
SELECT
    o.order_id,
    o.order_status,
    c.customer_city,
    c.customer_state
FROM raw.raw_orders AS o
LEFT JOIN raw.raw_customers AS c
    ON o.customer_id = c.customer_id;

-- 2. Orders by customer state
SELECT
    c.customer_state,
    COUNT(*) AS order_count
FROM raw.raw_orders AS o
LEFT JOIN raw.raw_customers AS c
    ON o.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY order_count DESC;