{{
    config(
        materialized='view'
    )
}}

with source_data as (
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
        geometry_wkt,
        loaded_at
    from {{ source('osm', 'raw_countries') }}
)

select
    osm_id,
    osm_type,
    name,
    iso3166_1_alpha2,
    iso3166_1_alpha3,
    -- Try to parse population as integer, handle nulls
    try_cast(population as integer) as population,
    capital,
    wikidata,
    wikipedia,
    official_name,
    latitude,
    longitude,
    -- Convert WKT to geometry using DuckDB spatial extension
    case 
        when geometry_wkt is not null 
        then st_geomfromtext(geometry_wkt)
        else null
    end as geometry,
    geometry_wkt,
    loaded_at
from source_data
