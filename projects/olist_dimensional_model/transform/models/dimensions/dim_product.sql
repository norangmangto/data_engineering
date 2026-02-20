/*
    dim_product

    Product dimension enriched with English category names.
*/

with products as (
    select
        p.product_id,
        p.product_category_name        as category_name_pt,
        t.product_category_name_english as category_name_en,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm
    from {{ ref('stg_products') }} p
    left join {{ ref('stg_product_category_translation') }} t
        on p.product_category_name = t.product_category_name
)

select * from products
