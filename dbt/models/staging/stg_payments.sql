select
    cast(order_id as text) as order_id,
    cast(payment_sequential as integer) as payment_sequential,

    cast(order_id as text) || '-' || cast(payment_sequential as text) as payment_key,

    lower(trim(payment_type)) as payment_type,
    cast(nullif(payment_installments, '') as integer) as payment_installments,
    cast(payment_value as numeric(12, 2)) as payment_value,

    cast(ingested_at as timestamp) as ingested_at
from {{ source('raw', 'raw_payments') }}