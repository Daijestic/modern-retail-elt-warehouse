select
    order_id
from {{ ref('fact_orders') }}
where total_order_value < 0
