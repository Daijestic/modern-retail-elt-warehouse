select
    cast(order_id as text) as order_id,
    cast(customer_id as text) as customer_id,
    lower(trim(order_status)) as order_status,
    cast(nullif(order_purchase_timestamp, '') as timestamp) as order_purchase_timestamp,
    cast(nullif(order_approved_at, '') as timestamp) as order_approved_at,
    cast(nullif(order_delivered_customer_date, '') as timestamp) as order_delivered_customer_date,
    cast(nullif(order_estimated_delivery_date, '') as timestamp) as order_estimated_delivery_date,
    cast(_ingestion_run_id as text) as ingestion_run_id,
    cast(_source_file as text) as source_file,
    cast(_source_row_number as integer) as source_row_number,
    cast(_ingested_at as timestamp) as landing_ingested_at,
    cast(_file_checksum as text) as file_checksum,
    cast(_source_modified_at as timestamp) as source_modified_at
from {{ source('raw', 'raw_orders') }}
