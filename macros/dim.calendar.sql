{{ config(
    materialized = 'table',
    tags = ['dimension', 'calendar', 'banking']
) }}

with date_spine as (

    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "to_date('2010-01-01')",
        end_date   = "to_date('2035-12-31')"
    ) }}

),

holidays as (

    select
        holiday_date
    from {{ ref('bank_holidays') }}

),

calendar as (

    select
        cast(date_day as date)                 as cal_date,

        -- Date parts
        extract(year  from date_day)           as year,
        extract(month from date_day)           as month,
        extract(day   from date_day)           as day,

        extract(quarter from date_day)         as quarter,
        extract(week    from date_day)         as week_of_year,

        -- Day attributes
        dayofweek(date_day)                    as day_of_week,
        to_char(date_day, 'Day')               as day_name,
        to_char(date_day, 'Mon')               as month_name,

        -- Flags
        case when dayofweek(date_day) in (1,7) then true else false end as is_weekend,

        case
            when h.holiday_date is not null then true
            else false
        end as is_holiday,

        -- Business day logic (BANKING CORE)
        case
            when dayofweek(date_day) in (1,7) then false
            when h.holiday_date is not null then false
            else true
        end as is_business_day

    from date_spine d
    left join holidays h
        on d.date_day = h.holiday_date
)

select * from calendar
