select
    cast(customer_id as text) as customer_id,
    cast(customer_unique_id as text) as customer_unique_id,
    cast(customer_zip_code_prefix as text) as customer_zip_code_prefix,
    lower(trim(customer_city)) as customer_city,
    upper(trim(customer_state)) as customer_state,
    cast(_ingestion_run_id as text) as ingestion_run_id,
    cast(_source_file as text) as source_file,
    cast(_source_row_number as integer) as source_row_number,
    cast(_ingested_at as timestamp) as landing_ingested_at,
    cast(_file_checksum as text) as file_checksum,
    cast(_source_modified_at as timestamp) as source_modified_at
from {{ source('raw', 'raw_customers') }}
