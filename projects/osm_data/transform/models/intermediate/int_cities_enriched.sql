{{
    config(
        materialized='table'
    )
}}

with cities as (
    select * from {{ ref('stg_cities') }}
),

enriched_cities as (
    select
        osm_id,
        osm_type,
        name,
        country,
        population,
        wikidata,
        wikipedia,
        latitude,
        longitude,
        geometry,
        geometry_wkt,
        
        -- Add calculated fields
        case 
            when population >= 10000000 then 'Megacity'
            when population >= 1000000 then 'Large City'
            when population >= 100000 then 'Medium City'
            when population >= 10000 then 'Small City'
            else 'Town'
        end as city_size_category,
        
        -- Check if coordinates are available
        case 
            when latitude is not null and longitude is not null 
            then true 
            else false 
        end as has_coordinates,
        
        -- Check if geometry is available
        case 
            when geometry is not null 
            then true 
            else false 
        end as has_geometry,
        
        loaded_at
    from cities
)

select * from enriched_cities
