with source as (
    select * from {{ source('raw', 'olist_order_items_dataset') }}
)

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    cast(shipping_limit_date as timestamp) as shipping_limit_date,
    cast(price        as double) as price,
    cast(freight_value as double) as freight_value
from source
