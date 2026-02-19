with source as (
    select * from read_csv_auto('../data/raw/product_category_name_translation.csv')
)

select
    lower(trim(product_category_name))         as product_category_name,
    lower(trim(product_category_name_english))  as product_category_name_english
from source
