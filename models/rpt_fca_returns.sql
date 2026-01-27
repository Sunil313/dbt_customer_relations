{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "reporting_period",
      "data_type": "date"
    }
) }}

with policy_active_periods as (
    select
        policy_sk,
        policy_id,
        product_code,
        customer_id,
        policy_start_date,
        policy_end_date,
        is_current_record
    from {{ ref('dim_policy') }}
    where is_current_record = true
),

monthly_financials as (
    select
        policy_id,
        date_trunc('month', premium_date) as period,
        sum(premium_amount) as total_premium,
        sum(case when premium_type = 'EARNED' then premium_amount else 0 end) as earned_premium,
        sum(coalesce(incurred_amount, 0)) as total_claims
    from {{ ref('fct_premium') }} p
    left join {{ ref('fct_claims') }} c
      on p.policy_id = c.policy_id
      and date_trunc('month', p.premium_date) = date_trunc('month', c.claim_reported_date)
    group by 1, 2
),

risk_summary as (
    select
        policy_id,
        date_trunc('month', score_date) as period,
        avg(risk_score) as avg_risk_score
    from {{ ref('fct_risk_score') }}
    group by 1, 2
),

fca_base as (
    select
        mf.policy_id,
        mf.period as reporting_period,
        pa.product_code,
        mf.total_premium,
        mf.earned_premium,
        mf.total_claims,
        rs.avg_risk_score

    from monthly_financials mf
    join policy_active_periods pa
      on mf.policy_id = pa.policy_id
    left join risk_summary rs
      on mf.policy_id = rs.policy_id
     and mf.period = rs.period
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['policy_id', 'reporting_period']) }} as fca_return_sk,
        policy_id,
        product_code,
        reporting_period,
        total_premium,
        earned_premium,
        total_claims,
        avg_risk_score,
        current_timestamp as load_timestamp
    from fca_base
)

select * from final
