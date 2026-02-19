/*
    fact_beneficiary_summary

    Grain: one row per beneficiary per year (periodic snapshot).
    Contains annual summaries and chronic condition flags.
*/

select
    beneficiary_id,
    benefit_year,
    -- Join to date dimension (mid-year or first day of year as surrogate)
    cast(benefit_year || '0101' as integer) as snapshot_date_key,
    -- Join to geography
    state_code,
    county_code,
    part_a_months,
    part_b_months,
    part_d_months,
    hmo_months,
    chronic_alzheimer,
    chronic_heart_failure,
    chronic_kidney_disease,
    chronic_cancer,
    chronic_obstructive_pulmonary,
    chronic_depression,
    chronic_diabetes,
    chronic_ischemic_heart,
    chronic_osteoporosis,
    chronic_arthritis,
    chronic_stroke,
    reimbursement_inpatient,
    deductible_inpatient,
    primary_payer_inpatient,
    reimbursement_outpatient,
    deductible_outpatient,
    primary_payer_outpatient,
    reimbursement_carrier,
    deductible_carrier,
    primary_payer_carrier
from {{ ref('stg_beneficiary') }}
