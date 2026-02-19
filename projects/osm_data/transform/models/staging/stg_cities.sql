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
        country,
        population,
        wikidata,
        wikipedia,
        latitude,
        longitude,
        geometry_wkt,
        loaded_at
    from {{ source('osm', 'raw_cities') }}
)

select
    osm_id,
    osm_type,
    name,
    country,
    -- Try to parse population as integer, handle nulls
    try_cast(population as integer) as population,
    wikidata,
    wikipedia,
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
