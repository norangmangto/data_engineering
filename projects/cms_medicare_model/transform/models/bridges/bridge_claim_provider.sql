/*
    bridge_claim_provider

    Mappings claims to attending, operating, and other physicians.
*/

with base as (
    select claim_id, 'Inpatient' as claim_type, attending_physician_npi as provider_npi, 'Attending' as role from {{ ref('stg_inpatient_claims') }} where attending_physician_npi is not null
    union all
    select claim_id, 'Inpatient', operating_physician_npi, 'Operating' from {{ ref('stg_inpatient_claims') }} where operating_physician_npi is not null
    union all
    select claim_id, 'Inpatient', other_physician_npi, 'Other' from {{ ref('stg_inpatient_claims') }} where other_physician_npi is not null
    union all
    select claim_id, 'Outpatient', attending_physician_npi, 'Attending' from {{ ref('stg_outpatient_claims') }} where attending_physician_npi is not null
    union all
    select claim_id, 'Outpatient', operating_physician_npi, 'Operating' from {{ ref('stg_outpatient_claims') }} where operating_physician_npi is not null
    union all
    select claim_id, 'Outpatient', other_physician_npi, 'Other' from {{ ref('stg_outpatient_claims') }} where other_physician_npi is not null
)

select * from base
