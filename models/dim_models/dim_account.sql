{{ config(
    materialized = 'view'
) }}

select
  a.account_id,
  a.customer_id,
  a.product_id,
  a.currency_code,
  a.branch_id,
  a.account_open_date,
  a.account_close_date,
  a.account_status,
  a.opening_balance,
  current_timestamp as load_timestamp
from {{ ref('stg_accounts') }} a;
