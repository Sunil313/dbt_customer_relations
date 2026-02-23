{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append'
) }}

with premium_txns as (

    select
        premium_txn_id,
        policy_id,
        cast(posting_date as date) as premium_date,
        upper(premium_type) as premium_type,
        amount,
        currency_code
    from {{ ref('stg_premium_transactions') }}

    {% if is_incremental() %}
        where posting_date >
              (select max(premium_date) from {{ this }})
    {% endif %}

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
            'p.premium_txn_id'
        ]) }} as premium_sk,

        p.premium_txn_id,
        p.policy_id,
        cp.policy_sk,
        cp.customer_id,

        p.premium_date,
        p.premium_type,

        -- signed amount logic
        case
            when p.premium_type = 'REFUND' then -abs(p.amount)
            else abs(p.amount)
        end as premium_amount,

        p.currency_code,

        cp.product_code,
        c.risk_rating,

        current_timestamp as load_timestamp

    from premium_txns p
    join current_policy cp
        on p.policy_id = cp.policy_id
    left join customer_context c
        on cp.customer_id = c.customer_id

)

select * from final
