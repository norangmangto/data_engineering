/*
    bridge_claim_diagnosis

    Unpivots diagnosis codes from claims to handle many-to-many mapping.
*/

with inpatient as (
    {% for i in range(1, 11) %}
    select
        claim_id,
        'Inpatient' as claim_type,
        diagnosis_code_{{ i }} as diagnosis_code,
        {{ i }} as position,
        false as is_admitting
    from {{ ref('stg_inpatient_claims') }}
    where diagnosis_code_{{ i }} is not null
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
    union all
    select claim_id, 'Inpatient', admitting_diagnosis_code, 0, true from {{ ref('stg_inpatient_claims') }} where admitting_diagnosis_code is not null
),

outpatient as (
    {% for i in range(1, 11) %}
    select
        claim_id,
        'Outpatient' as claim_type,
        diagnosis_code_{{ i }} as diagnosis_code,
        {{ i }} as position,
        false as is_admitting
    from {{ ref('stg_outpatient_claims') }}
    where diagnosis_code_{{ i }} is not null
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
),

carrier as (
    {% for i in range(1, 9) %}
    select
        claim_id,
        'Carrier' as claim_type,
        diagnosis_code_{{ i }} as diagnosis_code,
        {{ i }} as position,
        false as is_admitting
    from {{ ref('stg_carrier_claims') }}
    where diagnosis_code_{{ i }} is not null
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

select * from inpatient
union all
select * from outpatient
union all
select * from carrier
