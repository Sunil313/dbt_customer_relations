{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

with policy_info as (
    select
        policy_id,
        product_code,
        is_current_record
    from {{ ref('dim_policy') }}
    where is_current_record = true
),

-- Aggregate premiums by period (written and earned)
premium_summary as (
    select
        policy_id,
        date_trunc('month', premium_date) as reporting_period,
        sum(case when premium_type = 'WRITTEN' then premium_amount else 0 end) as written_premium,
        sum(case when premium_type = 'EARNED' then premium_amount else 0 end) as earned_premium
    from {{ ref('fct_premium') }}
    group by 1, 2
),

-- Aggregate claims paid and incurred by period
claim_summary as (
    select
        policy_id,
        date_trunc('month', claim_reported_date) as reporting_period,
        sum(coalesce(paid_amount,0)) as total_paid,
        sum(coalesce(incurred_amount,0)) as total_incurred
    from {{ ref('fct_claims') }}
    group by 1, 2
),

-- Latest reserve snapshot per policy and period
reserve_summary as (
    select
        c.policy_id,
        date_trunc('month', r.snapshot_date) as reporting_period,
        sum(r.reserve_amount) as total_reserves
    from {{ ref('fct_claim_reserves') }} r
    join {{ ref('fct_claims') }} c
      on r.claim_id = c.claim_id
    group by 1, 2
),

-- (Optional) IFRS 17 specific contract margins, risk adjustment snapshots if available
-- Assuming staging model stg_ifrs_contract_metrics with policy_id, reporting_period, csm, risk_adjustment

ifrs_contract_metrics as (
    select
        policy_id,
        reporting_period,
        csm,
        risk_adjustment
    from {{ ref('stg_ifrs_contract_metrics') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['policy_id', 'reporting_period']) }} as ifrs_balance_sk,
        p.policy_id,
        p.product_code,
        ps.reporting_period,

        coalesce(ps.written_premium, 0) as written_premium,
        coalesce(ps.earned_premium, 0) as earned_premium,
        coalesce(cs.total_paid, 0) as claims_paid,
        coalesce(cs.total_incurred, 0) as claims_incurred,
        coalesce(rs.total_reserves, 0) as claim_reserves,

        icm.csm,
        icm.risk_adjustment,

        current_timestamp as load_timestamp

    from policy_info p
    left join premium_summary ps
      on p.policy_id = ps.policy_id
    left join claim_summary cs
      on p.policy_id = cs.policy_id
     and ps.reporting_period = cs.reporting_period
    left join reserve_summary rs
      on p.policy_id = rs.policy_id
     and ps.reporting_period = rs.reporting_period
    left join ifrs_contract_metrics icm
      on p.policy_id = icm.policy_id
     and ps.reporting_period = icm.reporting_period
)

select * from final