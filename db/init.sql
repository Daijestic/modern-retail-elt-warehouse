CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP NOT NULL,
    duration_seconds NUMERIC(12, 3) NOT NULL DEFAULT 0,
    tables_succeeded INTEGER NOT NULL DEFAULT 0,
    tables_partial INTEGER NOT NULL DEFAULT 0,
    tables_failed INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS metadata.ingestion_runs (
    table_run_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_file TEXT NOT NULL,
    target_schema TEXT NOT NULL,
    target_table TEXT NOT NULL,
    file_size_bytes BIGINT,
    file_checksum TEXT,
    schema_version TEXT,
    load_strategy TEXT NOT NULL,
    status TEXT NOT NULL,
    rows_read INTEGER NOT NULL DEFAULT 0,
    rows_loaded INTEGER NOT NULL DEFAULT 0,
    rows_rejected INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP NOT NULL,
    duration_seconds NUMERIC(12, 3) NOT NULL DEFAULT 0,
    error_type TEXT,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ingestion_runs_run_id
    ON metadata.ingestion_runs (run_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_ingestion_runs_status
    ON metadata.ingestion_runs (status, finished_at DESC);

CREATE TABLE IF NOT EXISTS metadata.ingestion_rejections (
    rejection_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    table_run_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    target_table TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    reason_code TEXT NOT NULL,
    reason_detail TEXT NOT NULL,
    raw_record JSONB NOT NULL,
    rejected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ingestion_rejections_run_id
    ON metadata.ingestion_rejections (run_id, target_table);

CREATE INDEX IF NOT EXISTS idx_ingestion_rejections_reason
    ON metadata.ingestion_rejections (reason_code, rejected_at DESC);

CREATE TABLE IF NOT EXISTS raw.raw_customers (
    customer_id TEXT PRIMARY KEY,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT,
    _ingestion_run_id TEXT NOT NULL,
    _source_file TEXT NOT NULL,
    _source_row_number INTEGER NOT NULL,
    _ingested_at TIMESTAMP NOT NULL,
    _file_checksum TEXT NOT NULL,
    _source_modified_at TIMESTAMP,
    ingested_at TIMESTAMP GENERATED ALWAYS AS (_ingested_at) STORED
);

CREATE TABLE IF NOT EXISTS raw.raw_orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TEXT,
    order_approved_at TEXT,
    order_delivered_customer_date TEXT,
    order_estimated_delivery_date TEXT,
    _ingestion_run_id TEXT NOT NULL,
    _source_file TEXT NOT NULL,
    _source_row_number INTEGER NOT NULL,
    _ingested_at TIMESTAMP NOT NULL,
    _file_checksum TEXT NOT NULL,
    _source_modified_at TIMESTAMP,
    ingested_at TIMESTAMP GENERATED ALWAYS AS (_ingested_at) STORED
);

CREATE TABLE IF NOT EXISTS raw.raw_order_items (
    order_id TEXT NOT NULL,
    order_item_id TEXT NOT NULL,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TEXT,
    price TEXT,
    freight_value TEXT,
    _ingestion_run_id TEXT NOT NULL,
    _source_file TEXT NOT NULL,
    _source_row_number INTEGER NOT NULL,
    _ingested_at TIMESTAMP NOT NULL,
    _file_checksum TEXT NOT NULL,
    _source_modified_at TIMESTAMP,
    ingested_at TIMESTAMP GENERATED ALWAYS AS (_ingested_at) STORED,
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
    _ingestion_run_id TEXT NOT NULL,
    _source_file TEXT NOT NULL,
    _source_row_number INTEGER NOT NULL,
    _ingested_at TIMESTAMP NOT NULL,
    _file_checksum TEXT NOT NULL,
    _source_modified_at TIMESTAMP,
    ingested_at TIMESTAMP GENERATED ALWAYS AS (_ingested_at) STORED
);

CREATE TABLE IF NOT EXISTS raw.raw_payments (
    order_id TEXT NOT NULL,
    payment_sequential TEXT NOT NULL,
    payment_type TEXT,
    payment_installments TEXT,
    payment_value TEXT,
    _ingestion_run_id TEXT NOT NULL,
    _source_file TEXT NOT NULL,
    _source_row_number INTEGER NOT NULL,
    _ingested_at TIMESTAMP NOT NULL,
    _file_checksum TEXT NOT NULL,
    _source_modified_at TIMESTAMP,
    ingested_at TIMESTAMP GENERATED ALWAYS AS (_ingested_at) STORED,
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE IF NOT EXISTS raw.raw_shipments (
    order_id TEXT PRIMARY KEY,
    delivered_customer_date TEXT,
    estimated_delivery_date TEXT,
    _ingestion_run_id TEXT NOT NULL,
    _source_file TEXT NOT NULL,
    _source_row_number INTEGER NOT NULL,
    _ingested_at TIMESTAMP NOT NULL,
    _file_checksum TEXT NOT NULL,
    _source_modified_at TIMESTAMP,
    ingested_at TIMESTAMP GENERATED ALWAYS AS (_ingested_at) STORED
);
