/*
    dim_geography

    Dimension for state and county codes used in beneficiary summaries.
*/

with geo as (
    select distinct state_code, county_code from {{ ref('stg_beneficiary') }}
    where state_code is not null or county_code is not null
)

select
    -- Surrogate key for state/county combination
    row_number() over (order by state_code, county_code) as geography_key,
    state_code,
    county_code,
    'Synthetic State ' || state_code as state_name,
    'Synthetic County ' || county_code as county_name
from geo
