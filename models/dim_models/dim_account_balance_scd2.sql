{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'account_balance_scd2_sk',
    on_schema_change = 'sync_all_columns'
) }}

with base as (
  select
    account_id,
    balance_date,
    closing_balance
  from {{ ref('fct_account_balance_daily') }}
  {% if is_incremental() %}
    where balance_date >= dateadd(day, -30, current_date)
  {% endif %}
),

-- detect boundaries where balance changes (or at the first row)
marked as (
  select
    account_id,
    balance_date,
    closing_balance,
    case
      when lag(closing_balance) over (partition by account_id order by balance_date) = closing_balance
        then 0
      else 1
    end as is_change
  from base
),

-- assign groups to compress consecutive equal balances
grouped as (
  select
    account_id,
    balance_date,
    closing_balance,
    sum(is_change) over (partition by account_id order by balance_date
                         rows unbounded preceding) as grp
  from marked
),

-- aggregate each group to an effective period
periods as (
  select
    account_id,
    closing_balance,
    min(balance_date) as effective_start_date,
    max(balance_date) as effective_end_date
  from grouped
  group by account_id, closing_balance, grp
),

final as (
  select
    {{ dbt_utils.generate_surrogate_key([
      'account_id',
      'cast(effective_start_date as string)',
      'cast(effective_end_date as string)'
    ]) }} as account_balance_scd2_sk,
    account_id,
    closing_balance,
    effective_start_date,
    effective_end_date,
    case when effective_end_date = (select max(balance_date) from {{ ref('fct_account_balance_daily') }})
         then 1 else 0 end as is_current,
    current_timestamp as load_timestamp
  from periods
)

select * from final;
