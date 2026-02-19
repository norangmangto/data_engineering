with source as (
    select * from read_csv_auto('../data/raw/olist_order_payments_dataset.csv')
)

select
    order_id,
    payment_sequential,
    lower(trim(payment_type))                 as payment_type,
    cast(payment_installments as int)         as payment_installments,
    cast(payment_value        as double)      as payment_value
from source
