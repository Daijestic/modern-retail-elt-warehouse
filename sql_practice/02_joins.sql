-- Purpose: Practice JOINs across raw retail tables.

-- 1. Orders with customer location
SELECT
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp,
    c.customer_city,
    c.customer_state
FROM raw.raw_orders AS o
LEFT JOIN raw.raw_customers AS c
    ON o.customer_id = c.customer_id
LIMIT 50;

-- 2. Order items with product category
SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    p.product_category_name,
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value AS gross_revenue
FROM raw.raw_order_items AS oi
LEFT JOIN raw.raw_products AS p
    ON oi.product_id = p.product_id
LIMIT 50;

-- 3. Orders with payment information
SELECT
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp,
    p.payment_type,
    p.payment_installments,
    p.payment_value
FROM raw.raw_orders AS o
LEFT JOIN raw.raw_payments AS p
    ON o.order_id = p.order_id
LIMIT 50;

-- 4. Orders with shipment dates
SELECT
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp,
    s.delivered_customer_date,
    s.estimated_delivery_date
FROM raw.raw_orders AS o
LEFT JOIN raw.raw_shipments AS s
    ON o.order_id = s.order_id
LIMIT 50;

-- 5. Revenue by customer state
SELECT
    c.customer_state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.price + oi.freight_value) AS total_revenue
FROM raw.raw_orders AS o
JOIN raw.raw_customers AS c
    ON o.customer_id = c.customer_id
JOIN raw.raw_order_items AS oi
    ON o.order_id = oi.order_id
GROUP BY c.customer_state
ORDER BY total_revenue DESC;