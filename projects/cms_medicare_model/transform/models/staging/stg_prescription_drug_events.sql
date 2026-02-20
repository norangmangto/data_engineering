/*
    stg_prescription_drug_events
*/

with source as (
    select * from {{ source('raw', 'prescription_drug_events_sample_1') }}
)

select
    DESYNPUF_ID as beneficiary_id,
    PDE_ID as pde_id,
    cast(strptime(cast(SRVC_DT as string), '%Y%m%d') as date) as service_date,
    PROD_SRVC_ID as product_service_id,
    cast(QTY_DSPNSD_NUM as double) as quantity_dispensed,
    cast(DAYS_SUPLY_NUM as double) as days_supply,
    cast(PTNT_PAY_AMT as double) as patient_pay_amount,
    cast(TOT_RX_CST_AMT as double) as total_rx_cost_amount
from source
