select
    cast(order_id as text) as order_id,
    cast(order_item_id as integer) as order_item_id,
    cast(order_id as text) || '-' || cast(order_item_id as text) as order_item_key,
    cast(product_id as text) as product_id,
    cast(seller_id as text) as seller_id,
    cast(nullif(shipping_limit_date, '') as timestamp) as shipping_limit_date,
    cast(price as numeric(12, 2)) as price,
    cast(freight_value as numeric(12, 2)) as freight_value,
    cast(_ingestion_run_id as text) as ingestion_run_id,
    cast(_source_file as text) as source_file,
    cast(_source_row_number as integer) as source_row_number,
    cast(_ingested_at as timestamp) as landing_ingested_at,
    cast(_file_checksum as text) as file_checksum,
    cast(_source_modified_at as timestamp) as source_modified_at
from {{ source('raw', 'raw_order_items') }}
