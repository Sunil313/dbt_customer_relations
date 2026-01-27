{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'account_balance_sk'
) }}

with date_spine as (

    -- create date range per account
    select
        a.account_id,
        d.date_day as balance_date
    from {{ ref('stg_accounts') }} a
    join {{ ref('dim_calendar') }} d
        on d.date_day >= a.account_open_date
       and d.date_day <= current_date

),

transactions_daily as (

    select
        account_id,
        transaction_date,
        sum(signed_amount) as daily_net_amount
    from {{ ref('fct_transactions') }}

    {% if is_incremental() %}
        where transaction_date >= dateadd(day, -7, current_date)
    {% endif %}

    group by
        account_id,
        transaction_date

),

running_balance as (

    select
        ds.account_id,
        ds.balance_date,

        coalesce(td.daily_net_amount, 0) as daily_net_amount,

        sum(coalesce(td.daily_net_amount, 0)) over (
            partition by ds.account_id
            order by ds.balance_date
            rows unbounded preceding
        ) as cumulative_amount

    from date_spine ds
    left join transactions_daily td
        on ds.account_id = td.account_id
       and ds.balance_date = td.transaction_date

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'rb.account_id',
            'rb.balance_date'
        ]) }} as account_balance_sk,

        rb.account_id,
        rb.balance_date,

        a.currency_code,

        a.opening_balance
        + rb.cumulative_amount as closing_balance,

        lag(
            a.opening_balance + rb.cumulative_amount
        ) over (
            partition by rb.account_id
            order by rb.balance_date
        ) as opening_balance,

        current_timestamp as load_timestamp

    from running_balance rb
    join {{ ref('stg_accounts') }} a
        on rb.account_id = a.account_id

)

select * from final
