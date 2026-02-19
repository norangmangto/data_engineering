/*
    dim_beneficiary

    Conformed dimension for beneficiaries.
    Deduplicates yearly summary records to get the latest demographics.
*/

with latest_record as (
    select
        beneficiary_id,
        birth_date,
        death_date,
        sex_code,
        race_code,
        esrd_indicator,
        state_code,
        county_code,
        row_number() over (partition by beneficiary_id order by benefit_year desc) as rn
    from {{ ref('stg_beneficiary') }}
)

select
    beneficiary_id,
    birth_date,
    death_date,
    sex_code,
    race_code,
    esrd_indicator,
    state_code,
    county_code
from latest_record
where rn = 1
