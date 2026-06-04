-- Purpose: Manual SQL data quality checks for raw tables and marts.

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

-- 3. Duplicate product_id
SELECT
    product_id,
    COUNT(*) AS record_count
FROM raw.raw_products
GROUP BY product_id
HAVING COUNT(*) > 1;

-- 4. Null customer_id in orders
SELECT COUNT(*) AS null_customer_id_count
FROM raw.raw_orders
WHERE customer_id IS NULL;

-- 5. Null order_id in order items
SELECT COUNT(*) AS null_order_id_count
FROM raw.raw_order_items
WHERE order_id IS NULL;

-- 6. Orphan orders: orders without matching customer
SELECT
    o.order_id,
    o.customer_id
FROM raw.raw_orders AS o
LEFT JOIN raw.raw_customers AS c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- 7. Orphan order items: order_items without matching order
SELECT
    oi.order_id,
    COUNT(*) AS orphan_item_count
FROM raw.raw_order_items AS oi
LEFT JOIN raw.raw_orders AS o
    ON oi.order_id = o.order_id
WHERE o.order_id IS NULL
GROUP BY oi.order_id;

-- 8. Orphan order items: order_items without matching product
SELECT
    oi.product_id,
    COUNT(*) AS orphan_item_count
FROM raw.raw_order_items AS oi
LEFT JOIN raw.raw_products AS p
    ON oi.product_id = p.product_id
WHERE p.product_id IS NULL
GROUP BY oi.product_id;

-- 9. Negative item price
SELECT *
FROM raw.raw_order_items
WHERE price < 0;

-- 10. Negative freight value
SELECT *
FROM raw.raw_order_items
WHERE freight_value < 0;

-- 11. Negative payment value
SELECT *
FROM raw.raw_payments
WHERE payment_value < 0;

-- 12. Delivered date before purchase date
SELECT
    o.order_id,
    o.order_purchase_timestamp,
    s.delivered_customer_date
FROM raw.raw_orders AS o
JOIN raw.raw_shipments AS s
    ON o.order_id = s.order_id
WHERE s.delivered_customer_date < o.order_purchase_timestamp;

-- 13. Orders with missing purchase timestamp
SELECT COUNT(*) AS missing_purchase_timestamp_count
FROM raw.raw_orders
WHERE order_purchase_timestamp IS NULL;

-- 14. Marts daily revenue should not have negative revenue
SELECT *
FROM marts.mart_daily_revenue
WHERE total_revenue < 0;

-- 15. Marts product performance should not have negative revenue
SELECT *
FROM marts.mart_product_performance
WHERE total_revenue < 0;

-- 16. Delivery performance should not have negative delivery_days
SELECT *
FROM marts.mart_delivery_performance
WHERE delivery_days < 0;