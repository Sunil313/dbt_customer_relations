{% macro date_calculations(date_type, offset=1) %}

(
    select
        case

            -- BUSINESS DATE
            when '{{ date_type }}' = 'business_date' then
                {{ get_business_date() }}

            -- PREVIOUS BUSINESS DAY (T-1, T-2, ...)
            when '{{ date_type }}' = 'previous_business_date' then
                (
                    select max(cal_date)
                    from {{ ref('dim_calendar') }}
                    where is_business_day = true
                      and cal_date < {{ get_business_date() }}
                )

            when '{{ date_type }}' = 'business_date_offset' then
                (
                    select cal_date
                    from (
                        select
                            cal_date,
                            row_number() over (order by cal_date desc) as rn
                        from {{ ref('dim_calendar') }}
                        where is_business_day = true
                          and cal_date <= {{ get_business_date() }}
                    ) d
                    where rn = {{ offset }}
                )

            -- MONTH START / END
            when '{{ date_type }}' = 'month_start' then
                date_trunc('month', {{ get_business_date() }})

            when '{{ date_type }}' = 'month_end' then
                (
                    select max(cal_date)
                    from {{ ref('dim_calendar') }}
                    where cal_date >= date_trunc('month', {{ get_business_date() }})
                      and cal_date <  dateadd(month, 1, date_trunc('month', {{ get_business_date() }}))
                )

            -- QUARTER START / END
            when '{{ date_type }}' = 'quarter_start' then
                date_trunc('quarter', {{ get_business_date() }})

            when '{{ date_type }}' = 'quarter_end' then
                (
                    select max(cal_date)
                    from {{ ref('dim_calendar') }}
                    where cal_date >= date_trunc('quarter', {{ get_business_date() }})
                      and cal_date <  dateadd(quarter, 1, date_trunc('quarter', {{ get_business_date() }}))
                )

            -- YEAR START / END
            when '{{ date_type }}' = 'year_start' then
                date_trunc('year', {{ get_business_date() }})

            when '{{ date_type }}' = 'year_end' then
                (
                    select max(cal_date)
                    from {{ ref('dim_calendar') }}
                    where cal_date >= date_trunc('year', {{ get_business_date() }})
                      and cal_date <  dateadd(year, 1, date_trunc('year', {{ get_business_date() }}))
                )

        end
)

{% endmacro %}
