select
    cast(order_id as text) as order_id,
    cast(customer_id as text) as customer_id,
    lower(trim(order_status)) as order_status,

    cast(nullif(order_purchase_timestamp, '') as timestamp) as order_purchase_timestamp,
    cast(nullif(order_approved_at, '') as timestamp) as order_approved_at,
    cast(nullif(order_delivered_customer_date, '') as timestamp) as order_delivered_customer_date,
    cast(nullif(order_estimated_delivery_date, '') as timestamp) as order_estimated_delivery_date,

    cast(ingested_at as timestamp) as ingested_at
from {{ source('raw', 'raw_orders') }}