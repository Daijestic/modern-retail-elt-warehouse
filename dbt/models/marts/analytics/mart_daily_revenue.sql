select
    order_date,

    count(distinct order_id) as total_orders,
    sum(total_order_value) as total_revenue,

    case
        when count(distinct order_id) = 0 then 0
        else sum(total_order_value) / count(distinct order_id)
    end as average_order_value

from {{ ref('fact_orders') }}

where order_date is not null
  and order_status not in ('canceled', 'unavailable')

group by order_date