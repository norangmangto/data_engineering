/*
    dim_diagnosis

    Dimension for ICD-9 diagnosis codes.
*/

with all_codes as (
    -- Inpatient diagnosis codes 1-10
    select distinct diagnosis_code_1 as diagnosis_code from {{ ref('stg_inpatient_claims') }} where diagnosis_code_1 is not null
    union
    select distinct diagnosis_code_2 as diagnosis_code from {{ ref('stg_inpatient_claims') }} where diagnosis_code_2 is not null
    union
    select distinct diagnosis_code_3 as diagnosis_code from {{ ref('stg_inpatient_claims') }} where diagnosis_code_3 is not null
    union
    select distinct diagnosis_code_4 as diagnosis_code from {{ ref('stg_inpatient_claims') }} where diagnosis_code_4 is not null
    union
    select distinct diagnosis_code_5 as diagnosis_code from {{ ref('stg_inpatient_claims') }} where diagnosis_code_5 is not null
    union
    select distinct diagnosis_code_6 as diagnosis_code from {{ ref('stg_inpatient_claims') }} where diagnosis_code_6 is not null
    union
    select distinct diagnosis_code_7 as diagnosis_code from {{ ref('stg_inpatient_claims') }} where diagnosis_code_7 is not null
    union
    select distinct diagnosis_code_8 as diagnosis_code from {{ ref('stg_inpatient_claims') }} where diagnosis_code_8 is not null
    union
    select distinct diagnosis_code_9 as diagnosis_code from {{ ref('stg_inpatient_claims') }} where diagnosis_code_9 is not null
    union
    select distinct diagnosis_code_10 as diagnosis_code from {{ ref('stg_inpatient_claims') }} where diagnosis_code_10 is not null
    union
    -- Inpatient admitting diagnosis
    select distinct admitting_diagnosis_code as diagnosis_code from {{ ref('stg_inpatient_claims') }} where admitting_diagnosis_code is not null
    union
    -- Outpatient diagnosis codes 1-10
    select distinct diagnosis_code_1 as diagnosis_code from {{ ref('stg_outpatient_claims') }} where diagnosis_code_1 is not null
    union
    select distinct diagnosis_code_2 as diagnosis_code from {{ ref('stg_outpatient_claims') }} where diagnosis_code_2 is not null
    union
    select distinct diagnosis_code_3 as diagnosis_code from {{ ref('stg_outpatient_claims') }} where diagnosis_code_3 is not null
    union
    select distinct diagnosis_code_4 as diagnosis_code from {{ ref('stg_outpatient_claims') }} where diagnosis_code_4 is not null
    union
    select distinct diagnosis_code_5 as diagnosis_code from {{ ref('stg_outpatient_claims') }} where diagnosis_code_5 is not null
    union
    select distinct diagnosis_code_6 as diagnosis_code from {{ ref('stg_outpatient_claims') }} where diagnosis_code_6 is not null
    union
    select distinct diagnosis_code_7 as diagnosis_code from {{ ref('stg_outpatient_claims') }} where diagnosis_code_7 is not null
    union
    select distinct diagnosis_code_8 as diagnosis_code from {{ ref('stg_outpatient_claims') }} where diagnosis_code_8 is not null
    union
    select distinct diagnosis_code_9 as diagnosis_code from {{ ref('stg_outpatient_claims') }} where diagnosis_code_9 is not null
    union
    select distinct diagnosis_code_10 as diagnosis_code from {{ ref('stg_outpatient_claims') }} where diagnosis_code_10 is not null
    union
    -- Carrier diagnosis codes 1-8
    select distinct diagnosis_code_1 as diagnosis_code from {{ ref('stg_carrier_claims') }} where diagnosis_code_1 is not null
    union
    select distinct diagnosis_code_2 as diagnosis_code from {{ ref('stg_carrier_claims') }} where diagnosis_code_2 is not null
    union
    select distinct diagnosis_code_3 as diagnosis_code from {{ ref('stg_carrier_claims') }} where diagnosis_code_3 is not null
    union
    select distinct diagnosis_code_4 as diagnosis_code from {{ ref('stg_carrier_claims') }} where diagnosis_code_4 is not null
    union
    select distinct diagnosis_code_5 as diagnosis_code from {{ ref('stg_carrier_claims') }} where diagnosis_code_5 is not null
    union
    select distinct diagnosis_code_6 as diagnosis_code from {{ ref('stg_carrier_claims') }} where diagnosis_code_6 is not null
    union
    select distinct diagnosis_code_7 as diagnosis_code from {{ ref('stg_carrier_claims') }} where diagnosis_code_7 is not null
    union
    select distinct diagnosis_code_8 as diagnosis_code from {{ ref('stg_carrier_claims') }} where diagnosis_code_8 is not null
)

select
    diagnosis_code as icd9_code,
    'ICD-9 Diagnosis' as diagnosis_description -- Descriptions usually linked from external codebook
from all_codes
