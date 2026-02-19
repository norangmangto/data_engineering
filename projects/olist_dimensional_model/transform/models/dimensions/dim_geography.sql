/*
    dim_geography

    Deduplicated geography dimension.
    Multiple geolocation rows exist per zip code prefix; we average
    the coordinates and pick the most common city/state for each.
*/

with city_state_counts as (
    -- Count how often each (zip, city, state) combination appears
    select
        zip_code_prefix,
        city,
        state,
        count(*) as occurrence_count
    from {{ ref('stg_geolocation') }}
    group by zip_code_prefix, city, state
),

most_common as (
    -- Pick the most frequent city/state per zip code
    select
        zip_code_prefix,
        city,
        state,
        row_number() over (
            partition by zip_code_prefix
            order by occurrence_count desc
        ) as rn
    from city_state_counts
),

geo_avg as (
    -- Average coordinates per zip code
    select
        zip_code_prefix,
        avg(latitude)  as latitude,
        avg(longitude) as longitude
    from {{ ref('stg_geolocation') }}
    group by zip_code_prefix
),

final as (
    select
        m.zip_code_prefix,
        m.city,
        m.state,
        a.latitude,
        a.longitude
    from most_common m
    inner join geo_avg a
        on m.zip_code_prefix = a.zip_code_prefix
    where m.rn = 1
)

select * from final
