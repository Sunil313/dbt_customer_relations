{{ config(materialized = 'view') }}

select
  p.product_code,
  p.product_name,
  p.product_line,
  p.product_group,
  p.coverage_type,
  current_timestamp as load_timestamp
from {{ ref('stg_products') }} p;
