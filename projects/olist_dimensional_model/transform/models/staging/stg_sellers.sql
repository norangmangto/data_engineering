with source as (
    select * from read_csv_auto('../data/raw/olist_sellers_dataset.csv')
)

select
    seller_id,
    seller_zip_code_prefix,
    lower(trim(seller_city))  as seller_city,
    upper(trim(seller_state)) as seller_state
from source
