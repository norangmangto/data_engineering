/*
    dim_payment_type

    Payment type dimension with surrogate keys.
*/

with payment_types as (
    select distinct payment_type
    from {{ ref('stg_order_payments') }}
    where payment_type is not null
)

select
    row_number() over (order by payment_type) as payment_type_key,
    payment_type
from payment_types
