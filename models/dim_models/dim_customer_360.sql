{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'customer_sk'
) }}

with customer_snapshot as (

    select
        customer_id,
        first_name,
        last_name,
        date_of_birth,
        customer_type,
        kyc_status,
        risk_rating,
        country_code,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('customer_snapshot') }}

),

account_summary as (

    select
        customer_id,
        count(distinct account_id) as total_accounts,
        sum(case when account_status = 'ACTIVE' then 1 else 0 end) as active_accounts,
        min(account_open_date) as first_account_open_date,
        max(account_open_date) as latest_account_open_date
    from {{ ref('stg_accounts') }}
    group by customer_id

),

final as (

    select
        -- surrogate key (stable for each SCD record)
        {{ dbt_utils.generate_surrogate_key([
            'cs.customer_id',
            'cs.dbt_valid_from'
        ]) }} as customer_sk,

        cs.customer_id,

        cs.first_name,
        cs.last_name,
        cs.date_of_birth,
        cs.customer_type,

        cs.kyc_status,
        cs.risk_rating,
        cs.country_code,

        coalesce(a.total_accounts, 0) as total_accounts,
        coalesce(a.active_accounts, 0) as active_accounts,

        a.first_account_open_date,
        a.latest_account_open_date,

        cs.dbt_valid_from as customer_start_date,
        cs.dbt_valid_to as customer_end_date,

        case
            when cs.dbt_valid_to is null then true
            else false
        end as is_current_record

    from customer_snapshot cs
    left join account_summary a
        on cs.customer_id = a.customer_id

)

select * from final
