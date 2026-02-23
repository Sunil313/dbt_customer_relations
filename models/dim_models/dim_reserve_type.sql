{{ config(materialized = 'table') }}

with base as (
  select distinct reserve_type
  from {{ ref('stg_claim_reserves_snapshots') }}
),

final as (
  select
    {{ dbt_utils.generate_surrogate_key(['reserve_type']) }} as reserve_type_sk,
    reserve_type
  from base
)

select * from final;
