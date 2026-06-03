select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    ingested_at
from {{ ref('stg_customers') }}