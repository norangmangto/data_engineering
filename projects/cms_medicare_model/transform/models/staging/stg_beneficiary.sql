/*
    stg_beneficiary

    Unions the 2008, 2009, and 2010 beneficiary summary files.
*/

{% set years = ['2008', '2009', '2010'] %}

with unioned as (
    {% for year in years %}
    select
        '{{ year }}' as benefit_year,
        DESYNPUF_ID as beneficiary_id,
        BENE_BIRTH_DT as birth_date,
        BENE_DEATH_DT as death_date,
        BENE_SEX_IDENT_CD as sex_code,
        BENE_RACE_CD as race_code,
        BENE_ESRD_IND as esrd_indicator,
        SP_STATE_CODE as state_code,
        BENE_COUNTY_CD as county_code,
        BENE_HI_CVRAGE_TOT_MONS as part_a_months,
        BENE_SMI_CVRAGE_TOT_MONS as part_b_months,
        BENE_HMO_CVRAGE_TOT_MONS as hmo_months,
        PLAN_CVRG_MOS_NUM as part_d_months,
        SP_ALZHDMTA as chronic_alzheimer,
        SP_CHF as chronic_heart_failure,
        SP_CHRNKIDN as chronic_kidney_disease,
        SP_CNCR as chronic_cancer,
        SP_COPD as chronic_obstructive_pulmonary,
        SP_DEPRESSN as chronic_depression,
        SP_DIABETES as chronic_diabetes,
        SP_ISCHMCHT as chronic_ischemic_heart,
        SP_OSTEOPRS as chronic_osteoporosis,
        SP_RA_OA as chronic_arthritis,
        SP_STRKETIA as chronic_stroke,
        MEDREIMB_IP as reimbursement_inpatient,
        BENRES_IP as deductible_inpatient,
        PPPYMT_IP as primary_payer_inpatient,
        MEDREIMB_OP as reimbursement_outpatient,
        BENRES_OP as deductible_outpatient,
        PPPYMT_OP as primary_payer_outpatient,
        MEDREIMB_CAR as reimbursement_carrier,
        BENRES_CAR as deductible_carrier,
        PPPYMT_CAR as primary_payer_carrier
    from read_csv_auto('../data/raw/DE1_0_{{ year }}_Beneficiary_Summary_File_Sample_1.csv')
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

select
    benefit_year,
    beneficiary_id,
    cast(strptime(cast(birth_date as string), '%Y%m%d') as date) as birth_date,
    cast(strptime(case when cast(death_date as string) = '0' then null else cast(death_date as string) end, '%Y%m%d') as date) as death_date,
    cast(sex_code as int) as sex_code,
    cast(race_code as int) as race_code,
    esrd_indicator,
    state_code,
    county_code,
    cast(part_a_months as int) as part_a_months,
    cast(part_b_months as int) as part_b_months,
    cast(hmo_months as int) as hmo_months,
    cast(part_d_months as int) as part_d_months,
    -- Mapping chronic condition indicators (1=Yes, 2=No) to boolean
    case when cast(chronic_alzheimer as int) = 1 then true else false end as chronic_alzheimer,
    case when cast(chronic_heart_failure as int) = 1 then true else false end as chronic_heart_failure,
    case when cast(chronic_kidney_disease as int) = 1 then true else false end as chronic_kidney_disease,
    case when cast(chronic_cancer as int) = 1 then true else false end as chronic_cancer,
    case when cast(chronic_obstructive_pulmonary as int) = 1 then true else false end as chronic_obstructive_pulmonary,
    case when cast(chronic_depression as int) = 1 then true else false end as chronic_depression,
    case when cast(chronic_diabetes as int) = 1 then true else false end as chronic_diabetes,
    case when cast(chronic_ischemic_heart as int) = 1 then true else false end as chronic_ischemic_heart,
    case when cast(chronic_osteoporosis as int) = 1 then true else false end as chronic_osteoporosis,
    case when cast(chronic_arthritis as int) = 1 then true else false end as chronic_arthritis,
    case when cast(chronic_stroke as int) = 1 then true else false end as chronic_stroke,
    cast(reimbursement_inpatient as double) as reimbursement_inpatient,
    cast(deductible_inpatient as double) as deductible_inpatient,
    cast(primary_payer_inpatient as double) as primary_payer_inpatient,
    cast(reimbursement_outpatient as double) as reimbursement_outpatient,
    cast(deductible_outpatient as double) as deductible_outpatient,
    cast(primary_payer_outpatient as double) as primary_payer_outpatient,
    cast(reimbursement_carrier as double) as reimbursement_carrier,
    cast(deductible_carrier as double) as deductible_carrier,
    cast(primary_payer_carrier as double) as primary_payer_carrier
from unioned
