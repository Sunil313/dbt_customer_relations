{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'bankruptcy_id'
) }}

with source_data as (

    select
        account_id,
        customer_id,
        insolvency_code,
        cast(event_date as date) as event_date
    from {{ source('raw', 'insolvency_events') }}
    where insolvency_code in (
        'BANKRUPT',
        'IVA',
        'DRO',
        'ADMINISTRATION',
        'LIQUIDATION'
    )

),

ordered_events as (

    select
        *,
        lag(event_date) over (
            partition by account_id, customer_id
            order by event_date
        ) as prev_event_date
    from source_data

),

bankruptcy_groups as (

    select
        *,
        sum(
            case
                -- new bankruptcy cycle if gap > 1 day
                when prev_event_date is null
                     or datediff(day, prev_event_date, event_date) > 1
                then 1
                else 0
            end
        ) over (
            partition by account_id, customer_id
            order by event_date
            rows unbounded preceding
        ) as bankruptcy_group
    from ordered_events

),

bankruptcy_periods as (

    select
        account_id,
        customer_id,
        insolvency_code,
        bankruptcy_group,

        min(event_date) as bankruptcy_start_date,
        max(event_date) as bankruptcy_end_date

    from bankruptcy_groups
    group by
        account_id,
        customer_id,
        insolvency_code,
        bankruptcy_group

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'account_id',
            'customer_id',
            'bankruptcy_group'
        ]) }} as bankruptcy_id,

        account_id,
        customer_id,
        insolvency_code,

        bankruptcy_start_date,

        -- if the bankruptcy is ongoing, end_date = NULL
        case
            when bankruptcy_end_date = max(bankruptcy_end_date)
                 over (partition by account_id, customer_id)
            then null
            else bankruptcy_end_date
        end as bankruptcy_end_date,

        case
            when bankruptcy_end_date = max(bankruptcy_end_date)
                 over (partition by account_id, customer_id)
            then true
            else false
        end as is_currently_bankrupt

    from bankruptcy_periods

)

select * from final
