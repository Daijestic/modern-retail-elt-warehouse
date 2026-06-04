-- Purpose: Basic checks for raw tables and simple business overview.

-- 1. Count customers
SELECT COUNT(*) AS customer_count
FROM raw.raw_customers;

-- 2. Count orders
SELECT COUNT(*) AS order_count
FROM raw.raw_orders;

-- 3. Count order items
SELECT COUNT(*) AS order_item_count
FROM raw.raw_order_items;

-- 4. Count products
SELECT COUNT(*) AS product_count
FROM raw.raw_products;

-- 5. Orders by status
SELECT
    order_status,
    COUNT(*) AS order_count
FROM raw.raw_orders
GROUP BY order_status
ORDER BY order_count DESC;

-- 6. Customers by state
SELECT
    customer_state,
    COUNT(*) AS customer_count
FROM raw.raw_customers
GROUP BY customer_state
ORDER BY customer_count DESC;