with source as (
    select * from {{ source('raw', 'olist_order_payments_dataset') }}
)

select
    order_id,
    payment_sequential,
    lower(trim(payment_type))                 as payment_type,
    cast(payment_installments as int)         as payment_installments,
    cast(payment_value        as double)      as payment_value
from source
