{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append'
) }}

with transactions as (

    select
        transaction_sk,
        transaction_id,
        account_id,
        customer_id,
        transaction_ts,
        transaction_date,
        abs(amount) as amount,
        currency_code,
        transaction_country
    from {{ ref('fct_transactions') }}

    {% if is_incremental() %}
        where transaction_ts >
              (select max(transaction_ts) from {{ this }})
    {% endif %}

),

customer_context as (

    select
        customer_id,
        risk_rating
    from {{ ref('dim_customer_360') }}
    where is_current_record = true

),

country_risk as (

    select
        country_code,
        risk_level
    from {{ ref('seed_country_risk') }}

),

thresholds as (

    select
        max(case when rule_name = 'LARGE_TRANSACTION' then threshold_value end) as large_txn_limit,
        max(case when rule_name = 'HIGH_VELOCITY_TXN' then threshold_value end) as velocity_limit
    from {{ ref('seed_aml_thresholds') }}

),

velocity_check as (

    select
        account_id,
        transaction_date,
        count(*) as txn_count
    from transactions
    group by account_id, transaction_date

),

flagged as (

    select
        t.transaction_sk,
        t.transaction_id,
        t.account_id,
        t.customer_id,
        t.transaction_ts,
        t.amount,
        t.currency_code,

        c.risk_rating,
        cr.risk_level as country_risk,

        case
            when t.amount >= th.large_txn_limit then 'LARGE_TRANSACTION'
            when cr.risk_level = 'HIGH' then 'HIGH_RISK_COUNTRY'
            when v.txn_count >= th.velocity_limit then 'HIGH_VELOCITY'
        end as rule_triggered,

        case
            when t.amount >= th.large_txn_limit then 'HIGH'
            when cr.risk_level = 'HIGH' then 'HIGH'
            when v.txn_count >= th.velocity_limit then 'MEDIUM'
            else 'LOW'
        end as alert_severity

    from transactions t
    join customer_context c
        on t.customer_id = c.customer_id
    left join country_risk cr
        on t.transaction_country = cr.country_code
    left join velocity_check v
        on t.account_id = v.account_id
       and t.transaction_date = v.transaction_date
    cross join thresholds th

    where
        t.amount >= th.large_txn_limit
        or cr.risk_level = 'HIGH'
        or v.txn_count >= th.velocity_limit

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'transaction_sk',
            'rule_triggered'
        ]) }} as suspicious_activity_sk,

        transaction_id,
        transaction_sk,
        account_id,
        customer_id,

        rule_triggered,
        alert_severity,

        transaction_ts,
        amount,
        currency_code,

        current_timestamp as load_timestamp

    from flagged

)

select * from final