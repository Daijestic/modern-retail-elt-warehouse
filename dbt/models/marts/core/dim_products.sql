select
    product_id,
    coalesce(product_category_name, 'unknown') as product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    ingestion_run_id,
    source_file,
    source_row_number,
    landing_ingested_at
from {{ ref('stg_products') }}
