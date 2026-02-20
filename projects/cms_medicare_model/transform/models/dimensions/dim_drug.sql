/*
    dim_drug

    Dimension for prescription drugs based on product service IDs.
*/

with drugs as (
    select distinct product_service_id from {{ ref('stg_prescription_drug_events') }} where product_service_id is not null
)

select
    product_service_id as drug_id,
    'Synthetic Drug' as drug_name -- Names require NDC lookup
from drugs
