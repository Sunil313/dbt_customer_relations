{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append'
) }}

with claims as (

    select
        claim_id,
        policy_id,
        cast(claim_reported_date as date) as claim_reported_date,
        cast(loss_date as date) as loss_date,
        claim_type,
        claim_status
    from {{ ref('stg_claims') }}

    {% if is_incremental() %}
        where claim_reported_date >
              (select max(claim_reported_date) from {{ this }})
    {% endif %}

),

claim_payments as (

    select
        claim_id,
        sum(payment_amount) as total_paid_amount
    from {{ ref('stg_claim_payments') }}
    group by claim_id

),

claim_reserves as (

    select
        claim_id,
        max(reserve_amount) as current_reserve_amount
    from {{ ref('stg_claim_reserves') }}
    group by claim_id

),

current_policy as (

    select
        policy_id,
        policy_sk,
        customer_id,
        product_code
    from {{ ref('dim_policy') }}
    where is_current_record = true

),

customer_context as (

    select
        customer_id,
        risk_rating
    from {{ ref('dim_customer_360') }}
    where is_current_record = true

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'c.claim_id'
        ]) }} as claim_sk,

        c.claim_id,
        c.policy_id,
        p.policy_sk,
        p.customer_id,

        c.claim_reported_date,
        c.loss_date,

        c.claim_type,
        c.claim_status,

        coalesce(cp.total_paid_amount, 0) as paid_amount,
        coalesce(cr.current_reserve_amount, 0) as reserve_amount,

        -- ultimate claim cost
        coalesce(cp.total_paid_amount, 0)
        + coalesce(cr.current_reserve_amount, 0) as incurred_amount,

        p.product_code,
        cust.risk_rating,

        current_timestamp as load_timestamp

    from claims c
    joi
