{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'policy_sk'
) }}

with policy_snapshot as (

    select
        policy_id,
        customer_id,
        policy_status,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('policy_snapshot') }}

),

policy_attributes as (

    select
        policy_id,
        policy_number,
        product_code,
        policy_start_date,
        policy_end_date,
        premium_amount,
        currency_code
    from {{ ref('stg_policies') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'ps.policy_id',
            'ps.dbt_valid_from'
        ]) }} as policy_sk,

        ps.policy_id,
        pa.policy_number,
        ps.customer_id,

        pa.product_code,
        ps.policy_status,

        pa.policy_start_date,
        pa.policy_end_date,

        pa.premium_amount,
        pa.currency_code,

        ps.dbt_valid_from as status_start_date,
        ps.dbt_valid_to   as status_end_date,

        case
            when ps.dbt_valid_to is null then true
            else false
        end as is_current_record,

        case
            when ps.policy_status = 'ACTIVE' then true
            else false
        end as is_active_policy,

        case
            when ps.policy_status in ('CANCELLED', 'EXPIRED', 'LAPSED')
            then true
            else false
        end as is_terminated_policy

    from policy_snapshot ps
    left join policy_attributes pa
        on ps.policy_id = pa.policy_id

)

select * from final
