{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'account_lifecycle_sk'
) }}

with account_snapshot as (

    select
        account_id,
        customer_id,
        account_status,
        product_code,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('account_snapshot') }}

),

account_attributes as (

    select
        account_id,
        account_open_date,
        account_close_date,
        branch_code,
        currency_code
    from {{ ref('stg_accounts') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            's.account_id',
            's.dbt_valid_from'
        ]) }} as account_lifecycle_sk,

        s.account_id,
        s.customer_id,

        s.account_status,
        s.product_code,

        a.branch_code,
        a.currency_code,

        a.account_open_date,
        a.account_close_date,

        s.dbt_valid_from as status_start_date,
        s.dbt_valid_to   as status_end_date,

        case
            when s.dbt_valid_to is null then true
            else false
        end as is_current_status,

        case
            when s.account_status = 'OPEN' then true
            else false
        end as is_opened,

        case
            when s.account_status = 'ACTIVE' then true
            else false
        end as is_active,

        case
            when s.account_status = 'DORMANT' then true
            else false
        end as is_dormant,

        case
            when s.account_status = 'CLOSED' then true
            else false
        end as is_closed

    from account_snapshot s
    left join account_attributes a
        on s.account_id = a.account_id

)

select * from final
