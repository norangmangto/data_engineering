/*
    fact_carrier_claims

    Grain: one row per carrier/physician claim.
*/

select
    claim_id,
    beneficiary_id,
    cast(strftime(claim_from_date, '%Y%m%d') as integer) as claim_from_date_key,
    cast(strftime(claim_thru_date, '%Y%m%d') as integer) as claim_thru_date_key,
    claim_payment_amount,
    primary_payer_paid_amount
from {{ ref('stg_carrier_claims') }}
