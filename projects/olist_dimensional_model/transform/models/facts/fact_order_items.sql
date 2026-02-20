/*
    fact_order_items

    Grain: one row per order item.
    Joins order items with orders, customer dimension (SCD2-aware),
    and computes delivery performance metrics.
*/

with order_items as (
    select
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.seller_id,
        oi.price,
        oi.freight_value,
        oi.shipping_limit_date,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date
    from {{ ref('stg_order_items') }} oi
    inner join {{ ref('stg_orders') }} o
        on oi.order_id = o.order_id
),

with_customer as (
    select
        oi.*,
        c.customer_key,
        c.customer_zip_code_prefix as customer_zip
    from order_items oi
    inner join {{ ref('stg_customers') }} sc
        on oi.customer_id = sc.customer_id
    inner join {{ ref('dim_customer') }} c
        on sc.customer_unique_id = c.customer_unique_id
        -- SCD2 join: match the version that was active at the time of the order
        and cast(oi.order_purchase_timestamp as date) between c.valid_from and c.valid_to
),

final as (
    select
        wc.order_id,
        wc.order_item_id,
        wc.customer_key,
        wc.product_id,
        wc.seller_id,
        cast(strftime(wc.order_purchase_timestamp, '%Y%m%d') as int) as order_date_key,
        wc.customer_zip                                               as customer_geo_key,
        wc.order_status,
        wc.price,
        wc.freight_value,
        wc.shipping_limit_date,
        wc.order_approved_at,
        wc.order_delivered_carrier_date,
        wc.order_delivered_customer_date,
        wc.order_estimated_delivery_date,
        -- Delivery performance metrics
        case
            when wc.order_delivered_customer_date is not null
                 and wc.order_purchase_timestamp is not null
            then date_diff(
                'day',
                cast(wc.order_purchase_timestamp as date),
                cast(wc.order_delivered_customer_date as date)
            )
        end as delivery_days,
        case
            when wc.order_estimated_delivery_date is not null
                 and wc.order_purchase_timestamp is not null
            then date_diff(
                'day',
                cast(wc.order_purchase_timestamp as date),
                cast(wc.order_estimated_delivery_date as date)
            )
        end as estimated_delivery_days,
        case
            when wc.order_delivered_customer_date is not null
                 and wc.order_estimated_delivery_date is not null
                 and wc.order_delivered_customer_date > wc.order_estimated_delivery_date
            then true
            else false
        end as is_late
    from with_customer wc
)

select * from final
