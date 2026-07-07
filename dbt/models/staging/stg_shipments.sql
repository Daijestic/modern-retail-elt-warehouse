select
    cast(order_id as text) as order_id,
    cast(nullif(delivered_customer_date, '') as timestamp) as delivered_customer_date,
    cast(nullif(estimated_delivery_date, '') as timestamp) as estimated_delivery_date,
    cast(_ingestion_run_id as text) as ingestion_run_id,
    cast(_source_file as text) as source_file,
    cast(_source_row_number as integer) as source_row_number,
    cast(_ingested_at as timestamp) as landing_ingested_at,
    cast(_file_checksum as text) as file_checksum,
    cast(_source_modified_at as timestamp) as source_modified_at
from {{ source('raw', 'raw_shipments') }}
