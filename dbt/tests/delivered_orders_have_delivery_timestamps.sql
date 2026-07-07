select
    order_id
from {{ ref('fact_orders') }}
where order_status = 'delivered'
  and delivered_customer_date is null
