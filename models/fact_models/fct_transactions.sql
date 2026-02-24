{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append'
) }}


with base_transactions as (

    select
        transaction_id,
        account_id,
        cast(transaction_timestamp as timestamp) as transaction_ts,
        cast(transaction_timestamp as date) as transaction_date,
        amount,
        currency_code,
        upper(transaction_type) as transaction_type,
        merchant_name,
        country_code
    from {{ ref('stg_transactions') }}

    {% if is_incremental() %}
        where transaction_timestamp >
              (select max(transaction_ts) from {{ this }})
    {% endif %}

),

account_context as (

    select
        account_id,
        customer_id,
        product_code
    from {{ ref('dim_account_lifecycle') }}
    where is_current_status = true

),

customer_context as (

    select
        customer_id,
        risk_rating,
        country_code as customer_country
    from {{ ref('dim_customer_360') }}
    where is_current_record = true

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            't.transaction_id'
        ]) }} as transaction_sk,

        t.transaction_id,
        t.account_id,
        a.customer_id,

        t.transaction_ts,
        t.transaction_date,

        t.amount,
        t.currency_code,
        t.transaction_type,

        case
            when t.transaction_type = 'DEBIT' then -abs(t.amount)
            else abs(t.amount)
        end as signed_amount,

        t.merchant_name,
        t.country_code as transaction_country,

        a.product_code,
        c.risk_rating,
        c.customer_country,

        current_timestamp as load_timestamp

    from base_transactions t
    left join account_context a
        on t.account_id = a.account_id
    left join customer_context c
        on a.customer_id = c.customer_id

)

select * from final
