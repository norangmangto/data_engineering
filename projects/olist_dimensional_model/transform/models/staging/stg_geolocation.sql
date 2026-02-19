with source as (
    select * from {{ source('raw', 'olist_geolocation_dataset') }}
)

select
    geolocation_zip_code_prefix as zip_code_prefix,
    cast(geolocation_lat as double) as latitude,
    cast(geolocation_lng as double) as longitude,
    lower(trim(geolocation_city))   as city,
    upper(trim(geolocation_state))  as state
from source
