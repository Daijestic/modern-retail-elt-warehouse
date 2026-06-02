CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.ingestion_runs (
    run_id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    target_table TEXT NOT NULL,
    row_count INTEGER,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS raw.raw_customers (
    customer_id TEXT PRIMARY KEY,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.raw_orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TEXT,
    order_approved_at TEXT,
    order_delivered_customer_date TEXT,
    order_estimated_delivery_date TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.raw_order_items (
    order_id TEXT NOT NULL,
    order_item_id TEXT NOT NULL,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TEXT,
    price NUMERIC,
    freight_value NUMERIC,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS raw.raw_products (
    product_id TEXT PRIMARY KEY,
    product_category_name TEXT,
    product_name_lenght TEXT,
    product_description_lenght TEXT,
    product_photos_qty TEXT,
    product_weight_g TEXT,
    product_length_cm TEXT,
    product_height_cm TEXT,
    product_width_cm TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.raw_payments (
    order_id TEXT NOT NULL,
    payment_sequential TEXT NOT NULL,
    payment_type TEXT,
    payment_installments TEXT,
    payment_value NUMERIC,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE IF NOT EXISTS raw.raw_shipments (
    order_id TEXT PRIMARY KEY,
    delivered_customer_date TEXT,
    estimated_delivery_date TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);