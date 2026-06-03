select
    cast(order_id as text) as order_id,

    cast(nullif(delivered_customer_date, '') as timestamp) as delivered_customer_date,
    cast(nullif(estimated_delivery_date, '') as timestamp) as estimated_delivery_date,

    cast(ingested_at as timestamp) as ingested_at
from {{ source('raw', 'raw_shipments') }}