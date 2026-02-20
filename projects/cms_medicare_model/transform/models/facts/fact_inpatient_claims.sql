/*
    fact_inpatient_claims

    Grain: one row per inpatient claim/stay.
*/

select
    claim_id,
    beneficiary_id,
    provider_id,
    cast(strftime(claim_from_date, '%Y%m%d') as integer) as claim_from_date_key,
    cast(strftime(claim_thru_date, '%Y%m%d') as integer) as claim_thru_date_key,
    cast(strftime(admission_date, '%Y%m%d') as integer) as admission_date_key,
    cast(strftime(discharge_date, '%Y%m%d') as integer) as discharge_date_key,
    drg_code,
    claim_payment_amount,
    primary_payer_paid_amount,
    beneficiary_deductible_amount,
    beneficiary_coinsurance_amount,
    beneficiary_blood_deductible_amount,
    utilization_day_count,
    datediff('day', admission_date, discharge_date) as length_of_stay
from {{ ref('stg_inpatient_claims') }}
