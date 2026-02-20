/*
    dim_customer — SCD Type 2

    Tracks customer address changes across orders.
    The Olist dataset assigns a new customer_id per order but uses
    customer_unique_id to identify the same person. Different orders
    from the same person may have different zip/city/state.

    This model detects address changes and creates versioned rows
    with valid_from / valid_to / is_current.
*/

with customer_orders as (
    -- Join customers to orders to get the order date for each address snapshot
    select
        c.customer_unique_id,
        c.customer_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        o.order_purchase_timestamp,
        row_number() over (
            partition by c.customer_unique_id
            order by o.order_purchase_timestamp
        ) as order_seq
    from {{ ref('stg_customers') }} c
    inner join {{ ref('stg_orders') }} o
        on c.customer_id = o.customer_id
),

address_changes as (
    -- Detect when a customer's address changed compared to their previous order
    select
        *,
        lag(customer_zip_code_prefix) over (
            partition by customer_unique_id order by order_seq
        ) as prev_zip,
        lag(customer_city) over (
            partition by customer_unique_id order by order_seq
        ) as prev_city,
        lag(customer_state) over (
            partition by customer_unique_id order by order_seq
        ) as prev_state
    from customer_orders
),

address_versions as (
    -- Mark a new version whenever the address differs from the previous order
    select
        *,
        case
            when order_seq = 1 then 1
            when customer_zip_code_prefix IS DISTINCT FROM prev_zip
                 or customer_city IS DISTINCT FROM prev_city
                 or customer_state IS DISTINCT FROM prev_state
            then 1
            else 0
        end as is_new_version
    from address_changes
),

version_groups as (
    -- Assign a version number to each address period
    select
        *,
        sum(is_new_version) over (
            partition by customer_unique_id
            order by order_seq
            rows between unbounded preceding and current row
        ) as address_version
    from address_versions
),

scd_records as (
    -- Collapse each version group into a single SCD record
    select
        customer_unique_id,
        -- Keep the first customer_id seen in this version
        first_value(customer_id) over (
            partition by customer_unique_id, address_version
            order by order_seq
        ) as customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        address_version,
        min(order_purchase_timestamp) over (
            partition by customer_unique_id, address_version
        ) as valid_from,
        max(order_purchase_timestamp) over (
            partition by customer_unique_id, address_version
        ) as version_last_order
    from version_groups
),

deduplicated as (
    select distinct
        customer_unique_id,
        customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        address_version,
        valid_from,
        version_last_order
    from scd_records
),

final as (
    select
        row_number() over (order by customer_unique_id, address_version) as customer_key,
        customer_unique_id,
        customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        cast(valid_from as date) as valid_from,
        cast(
            greatest(
                cast(valid_from as date),
                coalesce(
                    (lead(valid_from) over (
                        partition by customer_unique_id
                        order by address_version
                    ) - interval '1 day')::date,
                    date '9999-12-31'
                )
            ) as date
        ) as valid_to,
        case
            when lead(address_version) over (
                partition by customer_unique_id
                order by address_version
            ) is null then true
            else false
        end as is_current
    from deduplicated
)

select * from final
