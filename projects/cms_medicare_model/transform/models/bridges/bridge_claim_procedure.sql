/*
    bridge_claim_procedure

    Unpivots procedure codes from claims.
*/

with inpatient as (
    {% for i in range(1, 7) %}
    select
        claim_id,
        'Inpatient' as claim_type,
        procedure_code_{{ i }} as procedure_code,
        {{ i }} as position
    from {{ ref('stg_inpatient_claims') }}
    where procedure_code_{{ i }} is not null
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
),

outpatient as (
    {% for i in range(1, 7) %}
    select
        claim_id,
        'Outpatient' as claim_type,
        procedure_code_{{ i }} as procedure_code,
        {{ i }} as position
    from {{ ref('stg_outpatient_claims') }}
    where procedure_code_{{ i }} is not null
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
),

carrier as (
    {% for i in range(1, 6) %}
    select
        claim_id,
        'Carrier' as claim_type,
        hcpcs_code_{{ i }} as procedure_code,
        {{ i }} as position
    from {{ ref('stg_carrier_claims') }}
    where hcpcs_code_{{ i }} is not null
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

select * from inpatient
union all
select * from outpatient
union all
select * from carrier
