/*
    stg_carrier_claims

    Unions Carrier Claims Sample 1A and 1B.
*/

with unioned as (
    select * from {{ source('raw', 'carrier_claims_sample_1a') }}
    union all
    select * from {{ source('raw', 'carrier_claims_sample_1b') }}
)

select
    DESYNPUF_ID as beneficiary_id,
    CLM_ID as claim_id,
    cast(strptime(cast(CLM_FROM_DT as string), '%Y%m%d') as date) as claim_from_date,
    cast(strptime(cast(CLM_THRU_DT as string), '%Y%m%d') as date) as claim_thru_date,
    cast(CLM_PMT_AMT as double) as claim_payment_amount,
    cast(NCH_PRMRY_PYR_CLM_PD_AMT as double) as primary_payer_paid_amount,
    -- Carrier claims have multiple physicians/suppliers and diagnosis codes per line item
    -- But at the claim level, we have principal diagnosis and 8 others
    ICD9_DGNS_CD_1 as diagnosis_code_1,
    ICD9_DGNS_CD_2 as diagnosis_code_2,
    ICD9_DGNS_CD_3 as diagnosis_code_3,
    ICD9_DGNS_CD_4 as diagnosis_code_4,
    ICD9_DGNS_CD_5 as diagnosis_code_5,
    ICD9_DGNS_CD_6 as diagnosis_code_6,
    ICD9_DGNS_CD_7 as diagnosis_code_7,
    ICD9_DGNS_CD_8 as diagnosis_code_8,
    -- HCPCS codes per line (simplified to claim level here for now)
    HCPCS_CD_1 as hcpcs_code_1,
    HCPCS_CD_2 as hcpcs_code_2,
    HCPCS_CD_3 as hcpcs_code_3,
    HCPCS_CD_4 as hcpcs_code_4,
    HCPCS_CD_5 as hcpcs_code_5,
    -- Physicians
    cast(null as string) as attending_physician_npi, -- Carrier uses line-level provider/physician
    -- Carrier file has TAX_NUM, PRVDR_SPCLTY, etc. but less NPIs at the header level
    -- Carrier claims are a bit different from IP/OP as they are more line-oriented
    LINE_NCH_PMT_AMT_1 as line_payment_1,
    LINE_ALLOWED_CHRG_AMT_1 as line_allowed_1
from unioned
