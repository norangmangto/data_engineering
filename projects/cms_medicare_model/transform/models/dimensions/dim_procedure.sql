/*
    dim_procedure

    Dimension for procedure codes (ICD-9 or HCPCS).
*/

with inpatient_proc as (
    select distinct procedure_code_1 as procedure_code, 'ICD-9' as type from {{ ref('stg_inpatient_claims') }} where procedure_code_1 is not null
    union
    select distinct procedure_code_2 as procedure_code, 'ICD-9' as type from {{ ref('stg_inpatient_claims') }} where procedure_code_2 is not null
),

outpatient_hcpcs as (
    -- HCPCS are often in the specific line items/CPT fields, but we simplified in staging
    -- In a real scenario, we'd pull from all procedural fields.
    select distinct diagnosis_code_admitting as procedure_code, 'HCPCS/CPT' as type from {{ ref('stg_outpatient_claims') }} where diagnosis_code_admitting is not null
)

select
    procedure_code,
    type,
    'Medical Procedure' as procedure_description
from (
    select * from inpatient_proc
    union
    select * from outpatient_hcpcs
)
