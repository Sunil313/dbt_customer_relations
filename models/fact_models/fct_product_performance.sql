{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append'
) }}

with monthly_premiums as (

    select
        p.product_code,
        date_trunc('month', premium_date) as month,
        sum(premium_amount) as total_premium
    from {{ ref('fct_premium') }} p
    group by 1, 2

),

monthly_claims as (

    select
        c.product_code,
        date_trunc('month', claim_reported_date) as month,
        sum(incurred_amount) as total_incurred
    from {{ ref('fct_claims') }} c
    group by 1, 2

),

monthly_risk as (

    select
        r.entity_id,
        r.entity_type,
        r.risk_score,
        date_trunc('month', r.score_date) as month
    from {{ ref('fct_risk_score') }} r
    where r.entity_type = 'POLICY'

),

product_policies as (

    select distinct
        policy_id,
        product_code
    from {{ ref('dim_policy') }}
    where is_current_record = true

),

-- aggregate risk score per product and month (avg risk score)
avg_risk_per_product as (

    select
        p.product_code,
        mr.month,
        avg(risk_score) as avg_risk_score
    from monthly_risk mr
    join {{ ref('dim_policy') }} p
      on mr.entity_id = p.policy_id
    where p.is_current_record = true
    group by 1, 2

),

final as (

    select
        mp.product_code,
        mp.month,

        coalesce(mp.total_premium, 0) as total_premium,
        coalesce(mc.total_incurred, 0) as total_incurred,

        case
            when coalesce(mp.total_premium, 0) = 0 then null
            else round(mc.total_incurred / mp.total_premium, 4)
        end as loss_ratio,

        arp.avg_risk_score,

        current_timestamp as load_timestamp

    from monthly_premiums mp
    full outer join monthly_claims mc
      on mp.product_code = mc.product_code
     and mp.month = mc.month
    left join avg_risk_per_product arp
      on mp.product_code = arp.product_code
     and mp.month = arp.month

    {% if is_incremental() %}
      where mp.month >= date_trunc('month', dateadd(month, -12, current_date))
    {% endif %}
)

select * from final
