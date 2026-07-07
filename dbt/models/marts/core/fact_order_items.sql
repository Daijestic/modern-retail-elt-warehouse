select
    order_item_key,
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price as item_price,
    freight_value,
    price + freight_value as line_total_amount,
    ingestion_run_id,
    source_file,
    source_row_number,
    landing_ingested_at
from {{ ref('stg_order_items') }}
