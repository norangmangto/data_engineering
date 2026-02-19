/*
    fact_order_payments

    Grain: one row per payment transaction.
    An order may have multiple payments (split payments, installments).
*/

with payments as (
    select
        p.order_id,
        p.payment_sequential,
        p.payment_type,
        p.payment_installments,
        p.payment_value,
        o.order_purchase_timestamp
    from {{ ref('stg_order_payments') }} p
    inner join {{ ref('stg_orders') }} o
        on p.order_id = o.order_id
),

final as (
    select
        py.order_id,
        py.payment_sequential,
        cast(strftime(py.order_purchase_timestamp, '%Y%m%d') as int) as order_date_key,
        pt.payment_type_key,
        py.payment_installments,
        py.payment_value
    from payments py
    inner join {{ ref('dim_payment_type') }} pt
        on py.payment_type = pt.payment_type
)

select * from final
