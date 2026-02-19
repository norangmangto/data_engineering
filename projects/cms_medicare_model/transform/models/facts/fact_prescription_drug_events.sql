/*
    fact_prescription_drug_events

    Grain: one row per drug fill.
*/

select
    pde_id,
    beneficiary_id,
    product_service_id as drug_id,
    cast(strftime(service_date, '%Y%m%d') as integer) as service_date_key,
    quantity_dispensed,
    days_supply,
    patient_pay_amount,
    total_rx_cost_amount
from {{ ref('stg_prescription_drug_events') }}
