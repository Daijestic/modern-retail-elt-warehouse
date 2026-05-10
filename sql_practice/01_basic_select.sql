
-- 1. Count customers
SELECT COUNT(*) AS customer_count
FROM raw.raw_customers;

-- 2. Count orders
SELECT COUNT(*) AS order_count
FROM raw.raw_orders;

-- 3. Orders by status
SELECT
    order_status,
    COUNT(*) AS order_count
FROM raw.raw_orders
GROUP BY order_status
ORDER BY order_count DESC;