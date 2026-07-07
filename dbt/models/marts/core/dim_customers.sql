select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    ingestion_run_id,
    source_file,
    source_row_number,
    landing_ingested_at
from {{ ref('stg_customers') }}
