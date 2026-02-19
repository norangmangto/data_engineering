/*
    fact_outpatient_claims

    Grain: one row per outpatient visit/claim.
*/

select
    claim_id,
    beneficiary_id,
    provider_id,
    cast(strftime(claim_from_date, '%Y%m%d') as integer) as claim_from_date_key,
    cast(strftime(claim_thru_date, '%Y%m%d') as integer) as claim_thru_date_key,
    claim_payment_amount,
    primary_payer_paid_amount,
    beneficiary_deductible_amount,
    beneficiary_coinsurance_amount
from {{ ref('stg_outpatient_claims') }}
