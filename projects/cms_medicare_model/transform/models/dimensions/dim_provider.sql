/*
    dim_provider

    Conformed dimension for providers/physicians.
    Extracted from NPI fields across all claim types.
*/

with providers as (
    select distinct attending_physician_npi as provider_npi from {{ ref('stg_inpatient_claims') }} where attending_physician_npi is not null
    union
    select distinct operating_physician_npi as provider_npi from {{ ref('stg_inpatient_claims') }} where operating_physician_npi is not null
    union
    select distinct other_physician_npi as provider_npi from {{ ref('stg_inpatient_claims') }} where other_physician_npi is not null
    union
    select distinct attending_physician_npi as provider_npi from {{ ref('stg_outpatient_claims') }} where attending_physician_npi is not null
    union
    select distinct operating_physician_npi as provider_npi from {{ ref('stg_outpatient_claims') }} where operating_physician_npi is not null
    union
    select distinct other_physician_npi as provider_npi from {{ ref('stg_outpatient_claims') }} where other_physician_npi is not null
    -- Note: Carrier claims have line-level providers which are not mapped in header staged model yet
    -- but usually NPIs overlap.
)

select
    provider_npi as provider_id,
    'Unknown Provider' as provider_name -- Names are not in the synthetic dataset public version
from providers
