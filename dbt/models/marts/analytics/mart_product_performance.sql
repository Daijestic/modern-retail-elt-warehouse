select
    p.product_id,
    p.product_category_name,

    count(foi.order_item_key) as total_quantity,
    count(distinct foi.order_id) as total_orders,
    sum(foi.item_price) as total_item_revenue,
    sum(foi.freight_value) as total_freight_value,
    sum(foi.line_total_amount) as gross_order_value

from {{ ref('fact_order_items') }} foi
left join {{ ref('dim_products') }} p
    on foi.product_id = p.product_id

group by
    p.product_id,
    p.product_category_name
