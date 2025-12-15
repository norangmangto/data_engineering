{{
    config(
        materialized='table'
    )
}}

with countries as (
    select * from {{ ref('stg_countries') }}
),

enriched_countries as (
    select
        osm_id,
        osm_type,
        name,
        iso3166_1_alpha2,
        iso3166_1_alpha3,
        population,
        capital,
        wikidata,
        wikipedia,
        official_name,
        latitude,
        longitude,
        geometry,
        geometry_wkt,
        
        -- Add calculated fields
        case 
            when population is null then 'Unknown'
            when population >= 1000000000 then 'Very Large'
            when population >= 100000000 then 'Large'
            when population >= 10000000 then 'Medium'
            when population >= 1000000 then 'Small'
            else 'Very Small'
        end as population_category,
        
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
        
        -- Check if ISO codes are available
        case 
            when iso3166_1_alpha2 is not null and iso3166_1_alpha3 is not null
            then true
            else false
        end as has_iso_codes,
        
        loaded_at
    from countries
)

select * from enriched_countries
