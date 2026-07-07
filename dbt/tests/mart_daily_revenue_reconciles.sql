with mart_total as (
    select coalesce(sum(gross_order_value), 0) as total_revenue
    from {{ ref('mart_daily_revenue') }}
),
fact_total as (
    select coalesce(sum(total_order_value), 0) as total_revenue
    from {{ ref('fact_orders') }}
    where order_status not in ('canceled', 'unavailable')
      and order_date is not null
)
select
    mart_total.total_revenue as mart_total_revenue,
    fact_total.total_revenue as fact_total_revenue
from mart_total
cross join fact_total
where mart_total.total_revenue <> fact_total.total_revenue
