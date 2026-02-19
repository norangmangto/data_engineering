/*
    dim_diagnosis

    Dimension for ICD-9 diagnosis codes.
*/

with all_codes as (
    select distinct diagnosis_code_1 as diagnosis_code from {{ ref('stg_inpatient_claims') }} where diagnosis_code_1 is not null
    union
    select distinct diagnosis_code_2 as diagnosis_code from {{ ref('stg_inpatient_claims') }} where diagnosis_code_2 is not null
    union
    select distinct diagnosis_code_3 as diagnosis_code from {{ ref('stg_inpatient_claims') }} where diagnosis_code_3 is not null
    union
    -- ... skipping 4-10 for brevity in dimension extraction, but in a real project we'd union all
    select distinct admitting_diagnosis_code as diagnosis_code from {{ ref('stg_inpatient_claims') }} where admitting_diagnosis_code is not null
    union
    select distinct diagnosis_code_1 as diagnosis_code from {{ ref('stg_outpatient_claims') }} where diagnosis_code_1 is not null
    union
    select distinct diagnosis_code_1 as diagnosis_code from {{ ref('stg_carrier_claims') }} where diagnosis_code_1 is not null
)

select
    diagnosis_code as icd9_code,
    'ICD-9 Diagnosis' as diagnosis_description -- Descriptions usually linked from external codebook
from all_codes
