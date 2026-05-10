
-- 1. Duplicate customer_id
SELECT
    customer_id,
    COUNT(*) AS record_count
FROM raw.raw_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- 2. Duplicate order_id
SELECT
    order_id,
    COUNT(*) AS record_count
FROM raw.raw_orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- 3. Orders with missing customer_id
SELECT *
FROM raw.raw_orders
WHERE customer_id IS NULL;

-- 4. Orphan orders
SELECT o.*
FROM raw.raw_orders AS o
LEFT JOIN raw.raw_customers AS c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;