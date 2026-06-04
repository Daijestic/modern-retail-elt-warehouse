-- Purpose: Practice window functions for interview questions.

-- 1. Customer order number
SELECT
    customer_id,
    order_id,
    CAST(order_purchase_timestamp AS DATE) AS order_date,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY order_purchase_timestamp
    ) AS order_number
FROM raw.raw_orders
WHERE order_purchase_timestamp IS NOT NULL;

-- 2. Rank product categories by revenue
WITH category_revenue AS (
    SELECT
        p.product_category_name,
        SUM(oi.price + oi.freight_value) AS total_revenue
    FROM raw.raw_order_items AS oi
    LEFT JOIN raw.raw_products AS p
        ON oi.product_id = p.product_id
    GROUP BY p.product_category_name
)
SELECT
    product_category_name,
    total_revenue,
    RANK() OVER (
        ORDER BY total_revenue DESC
    ) AS revenue_rank
FROM category_revenue;

-- 3. Monthly revenue with previous month revenue
WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', o.order_purchase_timestamp)::date AS revenue_month,
        SUM(oi.price + oi.freight_value) AS total_revenue
    FROM raw.raw_orders AS o
    JOIN raw.raw_order_items AS oi
        ON o.order_id = oi.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY DATE_TRUNC('month', o.order_purchase_timestamp)::date
)
SELECT
    revenue_month,
    total_revenue,
    LAG(total_revenue) OVER (
        ORDER BY revenue_month
    ) AS previous_month_revenue,
    total_revenue - LAG(total_revenue) OVER (
        ORDER BY revenue_month
    ) AS revenue_change
FROM monthly_revenue
ORDER BY revenue_month;

-- 4. Top 3 product categories by revenue per customer state
WITH state_category_revenue AS (
    SELECT
        c.customer_state,
        p.product_category_name,
        SUM(oi.price + oi.freight_value) AS total_revenue
    FROM raw.raw_orders AS o
    JOIN raw.raw_customers AS c
        ON o.customer_id = c.customer_id
    JOIN raw.raw_order_items AS oi
        ON o.order_id = oi.order_id
    LEFT JOIN raw.raw_products AS p
        ON oi.product_id = p.product_id
    GROUP BY
        c.customer_state,
        p.product_category_name
),
ranked AS (
    SELECT
        customer_state,
        product_category_name,
        total_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY customer_state
            ORDER BY total_revenue DESC
        ) AS category_rank
    FROM state_category_revenue
)
SELECT
    customer_state,
    product_category_name,
    total_revenue,
    category_rank
FROM ranked
WHERE category_rank <= 3
ORDER BY customer_state, category_rank;

-- 5. Running revenue by date
WITH daily_revenue AS (
    SELECT
        CAST(o.order_purchase_timestamp AS DATE) AS order_date,
        SUM(oi.price + oi.freight_value) AS total_revenue
    FROM raw.raw_orders AS o
    JOIN raw.raw_order_items AS oi
        ON o.order_id = oi.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY CAST(o.order_purchase_timestamp AS DATE)
)
SELECT
    order_date,
    total_revenue,
    SUM(total_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_revenue
FROM daily_revenue
ORDER BY order_date;