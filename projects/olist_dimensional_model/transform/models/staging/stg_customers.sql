with source as (
    select * from read_csv_auto('../data/raw/olist_customers_dataset.csv')
)

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    lower(trim(customer_city))   as customer_city,
    upper(trim(customer_state))  as customer_state
from source
