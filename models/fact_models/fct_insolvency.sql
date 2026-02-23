{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'insolvency_sk'
) }}

with source_events as (

    select
        account_id,
        customer_id,
        insolvency_code,
        cast(event_date as date) as event_date,
        upper(event_status) as event_status
    from {{ ref('stg_insolvency_events') }}
    where insolvency_code in (
        select insolvency_code
        from {{ ref('seed_insolvency_codes') }}
    )

),

ordered_events as (

    select
        *,
        lag(event_date) over (
            partition by account_id
            order by event_date
        ) as prev_event_date
    from source_events

),

insolvency_groups as (

    select
        *,
        sum(
            case
                when prev_event_date is null
                     or datediff(day, prev_event_date, event_date) > 1
                then 1
                else 0
            end
        ) over (
            partition by account_id
            order by event_date
            rows unbounded preceding
        ) as insolvency_group
    from ordered_events

),

insolvency_periods as (

    select
        account_id,
        customer_id,
        insolvency_code,
        insolvency_group,

        min(event_date) as insolvency_start_date,
        max(event_date) as insolvency_end_date

    from insolvency_groups
    group by
        account_id,
        customer_id,
        insolvency_code,
        insolvency_group

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'account_id',
            'customer_id',
            'insolvency_group'
        ]) }} as insolvency_sk,

        account_id,
        customer_id,
        insolvency_code,

        insolvency_start_date,

        case
            when insolvency_end_date = max(insolvency_end_date)
                 over (partition by account_id)
            then null
            else insolvency_end_date
        end as insolvency_end_date,

        case
            when insolvency_end_date = max(insolvency_end_date)
                 over (partition by account_id)
            then true
            else false
        end as is_currently_insolvent,

        current_timestamp as load_timestamp

    from insolvency_periods

)

select * from final
