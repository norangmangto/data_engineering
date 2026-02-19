/*
    stg_outpatient_claims
*/

with source as (
    select * from read_csv_auto('../data/raw/DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.csv')
)

select
    DESYNPUF_ID as beneficiary_id,
    CLM_ID as claim_id,
    SEGMENT as segment,
    cast(strptime(cast(CLM_FROM_DT as string), '%Y%m%d') as date) as claim_from_date,
    cast(strptime(cast(CLM_THRU_DT as string), '%Y%m%d') as date) as claim_thru_date,
    PRVDR_NUM as provider_id,
    cast(CLM_PMT_AMT as double) as claim_payment_amount,
    cast(NCH_PRMRY_PYR_CLM_PD_AMT as double) as primary_payer_paid_amount,
    AT_PHYSN_NPI as attending_physician_npi,
    OP_PHYSN_NPI as operating_physician_npi,
    OT_PHYSN_NPI as other_physician_npi,
    cast(ADMTNG_ICD9_DGNS_CD as string) as admitting_diagnosis_code,
    cast(NCH_BENE_PTB_DDCTBL_AMT as double) as beneficiary_deductible_amount,
    cast(NCH_BENE_PTB_COINSRNC_AMT as double) as beneficiary_coinsurance_amount,
    -- Diagnosis codes 1-10
    ICD9_DGNS_CD_1 as diagnosis_code_1,
    ICD9_DGNS_CD_2 as diagnosis_code_2,
    ICD9_DGNS_CD_3 as diagnosis_code_3,
    ICD9_DGNS_CD_4 as diagnosis_code_4,
    ICD9_DGNS_CD_5 as diagnosis_code_5,
    ICD9_DGNS_CD_6 as diagnosis_code_6,
    ICD9_DGNS_CD_7 as diagnosis_code_7,
    ICD9_DGNS_CD_8 as diagnosis_code_8,
    ICD9_DGNS_CD_9 as diagnosis_code_9,
    ICD9_DGNS_CD_10 as diagnosis_code_10,
    -- Procedure codes 1-6
    ICD9_PRCDR_CD_1 as procedure_code_1,
    ICD9_PRCDR_CD_2 as procedure_code_2,
    ICD9_PRCDR_CD_3 as procedure_code_3,
    ICD9_PRCDR_CD_4 as procedure_code_4,
    ICD9_PRCDR_CD_5 as procedure_code_5,
    ICD9_PRCDR_CD_6 as procedure_code_6
from source
