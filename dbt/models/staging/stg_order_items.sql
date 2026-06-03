select
    cast(order_id as text) as order_id,
    cast(order_item_id as integer) as order_item_id,

    cast(order_id as text) || '-' || cast(order_item_id as text) as order_item_key,

    cast(product_id as text) as product_id,
    cast(seller_id as text) as seller_id,

    cast(nullif(shipping_limit_date, '') as timestamp) as shipping_limit_date,
    cast(price as numeric(12, 2)) as price,
    cast(freight_value as numeric(12, 2)) as freight_value,

    cast(ingested_at as timestamp) as ingested_at
from {{ source('raw', 'raw_order_items') }}