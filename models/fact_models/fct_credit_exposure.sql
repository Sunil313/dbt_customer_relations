{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'credit_exposure_sk'
) }}


with daily_balance as (

    select
        account_id,
        balance_date,
        closing_balance
    from {{ ref('fct_account_daily_balance') }}

    {% if is_incremental() %}
        where balance_date >= dateadd(day, -7, current_date)
    {% endif %}

),

current_account as (

    select
        account_id,
        customer_id,
        product_code
    from {{ ref('dim_account_lifecycle') }}
    where is_current_status = true

),

current_customer as (

    select
        customer_id,
        risk_rating
    from {{ ref('dim_customer_360') }}
    where is_current_record = true

),

credit_limit as (

    select
        account_id,
        credit_limit
    from {{ ref('stg_credit_limits') }}
    where limit_end_date is null

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'db.account_id',
            'db.balance_date'
        ]) }} as credit_exposure_sk,

        db.account_id,
        ca.customer_id,
        db.balance_date,

        db.closing_balance,

        cl.credit_limit,

        case
            when cl.credit_limit is not null
            then cl.credit_limit - db.closing_balance
            else null
        end as available_credit,

        case
            when cl.credit_limit is not null and cl.credit_limit > 0
            then round(abs(db.closing_balance) / cl.credit_limit, 4)
            else null
        end as utilisation_ratio,

        cc.risk_rating,

        case
            when db.closing_balance < 0 then true
            else false
        end as is_overdrawn,

        current_timestamp as load_timestamp

    from daily_balance db
    join current_account ca
        on db.account_id = ca.account_id
    left join credit_limit cl
        on db.account_id = cl.account_id
    left join current_customer cc
        on ca.customer_id = cc.customer_id

)

select * from final
