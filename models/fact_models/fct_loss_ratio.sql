{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

with monthly_claims as (
    select
        policy_id,
        date_trunc('month', claim_reported_date) as claim_month,
        sum(incurred_amount) as total_incurred
    from {{ ref('fct_claims') }}
    group by 1, 2
),

monthly_premiums as (
    select
        policy_id,
        date_trunc('month', premium_date) as premium_month,
        sum(premium_amount) as total_premium
    from {{ ref('fct_premium') }}
    group by 1, 2
),

policy_product as (
    select
        policy_id,
        product_code
    from {{ ref('dim_policy') }}
    where is_current_record = true
),

loss_ratio_calc as (
    select
        pc.policy_id,
        pc.product_code,
        mc.claim_month as period_month,
        coalesce(mc.total_incurred, 0) as total_incurred,
        coalesce(mp.total_premium, 0) as total_premium,
        case
            when coalesce(mp.total_premium, 0) = 0 then null
            else round(mc.total_incurred / mp.total_premium, 4)
        end as loss_ratio

    from policy_product pc
    left join monthly_claims mc
        on pc.policy_id = mc.policy_id
    left join monthly_premiums mp
        on pc.policy_id = mp.policy_id
       and mc.claim_month = mp.premium_month

    {% if is_incremental() %}
    where mc.claim_month >= date_trunc('month', dateadd(month, -12, current_date))
    {% endif %}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'policy_id',
            'period_month'
        ]) }} as loss_ratio_sk,
        policy_id,
        product_code,
        period_month,
        total_incurred,
        total_premium,
        loss_ratio,
        current_timestamp as load_timestamp
    from loss_ratio_calc
)

select * from final
