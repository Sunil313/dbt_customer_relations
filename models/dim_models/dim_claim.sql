{{ config(materialized = 'view') }}

select
  c.claim_id,
  c.claim_number,
  c.loss_date,
  c.reported_date,
  c.claim_status,
  c.cause_of_loss_code,
  c.loss_location_code,
  c.currency_code,
  current_timestamp as load_timestamp
from {{ ref('stg_claims') }} c;
