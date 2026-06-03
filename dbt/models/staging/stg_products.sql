select
    cast(product_id as text) as product_id,
    lower(trim(product_category_name)) as product_category_name,

    cast(nullif(product_name_lenght, '') as integer) as product_name_length,
    cast(nullif(product_description_lenght, '') as integer) as product_description_length,
    cast(nullif(product_photos_qty, '') as integer) as product_photos_qty,

    cast(nullif(product_weight_g, '') as numeric(12, 2)) as product_weight_g,
    cast(nullif(product_length_cm, '') as numeric(12, 2)) as product_length_cm,
    cast(nullif(product_height_cm, '') as numeric(12, 2)) as product_height_cm,
    cast(nullif(product_width_cm, '') as numeric(12, 2)) as product_width_cm,

    cast(ingested_at as timestamp) as ingested_at
from {{ source('raw', 'raw_products') }}