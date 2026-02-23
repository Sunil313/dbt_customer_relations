{{ config(materialized = 'view') }}

select
  cust.customer_id,
  cust.customer_type,      -- person / org
  cust.first_name,
  cust.last_name,
  cust.organization_name,
  cust.segment,
  cust.country_code,
  current_timestamp as load_timestamp
from {{ ref('stg_customers') }} cust;
