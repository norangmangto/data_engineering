with source as (
    select * from read_csv_auto('../data/raw/olist_products_dataset.csv')
)

select
    product_id,
    lower(trim(product_category_name)) as product_category_name,
    cast(product_name_lenght        as int)    as product_name_length,
    cast(product_description_lenght as int)    as product_description_length,
    cast(product_photos_qty         as int)    as product_photos_qty,
    cast(product_weight_g           as double) as product_weight_g,
    cast(product_length_cm          as double) as product_length_cm,
    cast(product_height_cm          as double) as product_height_cm,
    cast(product_width_cm           as double) as product_width_cm
from source
