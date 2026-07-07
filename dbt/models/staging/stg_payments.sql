select
    cast(order_id as text) as order_id,
    cast(payment_sequential as integer) as payment_sequential,
    cast(order_id as text) || '-' || cast(payment_sequential as text) as payment_key,
    lower(trim(payment_type)) as payment_type,
    cast(nullif(payment_installments, '') as integer) as payment_installments,
    cast(payment_value as numeric(12, 2)) as payment_value,
    cast(_ingestion_run_id as text) as ingestion_run_id,
    cast(_source_file as text) as source_file,
    cast(_source_row_number as integer) as source_row_number,
    cast(_ingested_at as timestamp) as landing_ingested_at,
    cast(_file_checksum as text) as file_checksum,
    cast(_source_modified_at as timestamp) as source_modified_at
from {{ source('raw', 'raw_payments') }}
