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
    cast(_ingestion_run_id as text) as ingestion_run_id,
    cast(_source_file as text) as source_file,
    cast(_source_row_number as integer) as source_row_number,
    cast(_ingested_at as timestamp) as landing_ingested_at,
    cast(_file_checksum as text) as file_checksum,
    cast(_source_modified_at as timestamp) as source_modified_at
from {{ source('raw', 'raw_products') }}
