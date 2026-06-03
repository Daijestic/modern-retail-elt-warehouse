with item_summary as (

    select
        order_id,
        count(*) as item_count,
        sum(item_price) as total_item_price,
        sum(freight_value) as total_freight_value,
        sum(gross_revenue) as total_order_value
    from {{ ref('fact_order_items') }}
    group by order_id

),

payment_summary as (

    select
        order_id,
        count(*) as payment_count,
        sum(payment_value) as total_payment_value
    from {{ ref('stg_payments') }}
    group by order_id

),

orders as (

    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        cast(order_purchase_timestamp as date) as order_date,
        order_approved_at,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        ingested_at
    from {{ ref('stg_orders') }}

),

shipments as (

    select
        order_id,
        delivered_customer_date,
        estimated_delivery_date
    from {{ ref('stg_shipments') }}

)

select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_date,
    o.order_approved_at,

    coalesce(s.delivered_customer_date, o.order_delivered_customer_date) as delivered_customer_date,
    coalesce(s.estimated_delivery_date, o.order_estimated_delivery_date) as estimated_delivery_date,

    case
        when coalesce(s.delivered_customer_date, o.order_delivered_customer_date) is not null
         and o.order_purchase_timestamp is not null
        then cast(coalesce(s.delivered_customer_date, o.order_delivered_customer_date) as date)
             - cast(o.order_purchase_timestamp as date)
        else null
    end as delivery_days,

    case
        when coalesce(s.delivered_customer_date, o.order_delivered_customer_date) is not null
         and coalesce(s.estimated_delivery_date, o.order_estimated_delivery_date) is not null
         and coalesce(s.delivered_customer_date, o.order_delivered_customer_date)
             > coalesce(s.estimated_delivery_date, o.order_estimated_delivery_date)
        then true
        else false
    end as is_late_delivery,

    coalesce(i.item_count, 0) as item_count,
    coalesce(i.total_item_price, 0) as total_item_price,
    coalesce(i.total_freight_value, 0) as total_freight_value,
    coalesce(i.total_order_value, 0) as total_order_value,

    coalesce(p.payment_count, 0) as payment_count,
    coalesce(p.total_payment_value, 0) as total_payment_value,

    o.ingested_at

from orders o
left join item_summary i
    on o.order_id = i.order_id
left join payment_summary p
    on o.order_id = p.order_id
left join shipments s
    on o.order_id = s.order_id