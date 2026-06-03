select
    cast(customer_id as text) as customer_id,
    cast(customer_unique_id as text) as customer_unique_id,
    cast(customer_zip_code_prefix as text) as customer_zip_code_prefix,
    lower(trim(customer_city)) as customer_city,
    upper(trim(customer_state)) as customer_state,
    cast(ingested_at as timestamp) as ingested_at
from {{ source('raw', 'raw_customers') }}