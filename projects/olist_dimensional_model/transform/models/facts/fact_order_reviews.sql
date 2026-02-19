/*
    fact_order_reviews

    Grain: one row per review.
    Joins to dim_customer (SCD2-aware); derives review_date_key.
*/

with reviews as (
    select
        r.review_id,
        r.order_id,
        r.review_score,
        r.review_comment_title,
        r.review_comment_message,
        r.review_creation_date,
        r.review_answer_timestamp,
        o.customer_id,
        o.order_purchase_timestamp
    from {{ ref('stg_order_reviews') }} r
    inner join {{ ref('stg_orders') }} o
        on r.order_id = o.order_id
),

with_customer as (
    select
        rv.*,
        c.customer_key
    from reviews rv
    inner join {{ ref('stg_customers') }} sc
        on rv.customer_id = sc.customer_id
    inner join {{ ref('dim_customer') }} c
        on sc.customer_unique_id = c.customer_unique_id
        and cast(rv.order_purchase_timestamp as date) between c.valid_from and c.valid_to
),

final as (
    select
        wc.review_id,
        wc.order_id,
        wc.customer_key,
        cast(strftime(wc.review_creation_date, '%Y%m%d') as int) as review_date_key,
        wc.review_score,
        wc.review_comment_title,
        wc.review_comment_message
    from with_customer wc
)

select * from final
