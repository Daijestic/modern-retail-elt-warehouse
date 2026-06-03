select
    order_id,
    customer_id,
    order_status,
    order_date,

    delivered_customer_date,
    estimated_delivery_date,

    delivery_days,
    is_late_delivery,

    case
        when is_late_delivery = true then 1
        else 0
    end as late_delivery_flag

from {{ ref('fact_orders') }}

where delivered_customer_date is not null
   or estimated_delivery_date is not null