{% macro history_date_pull(
        lookback_days=5,
        default_start_date="'2000-01-01'"
    ) %}

(
    {% if is_incremental() %}

        -- Incremental run: pull last N business days
        select min(cal_date)
        from (
            select
                cal_date,
                row_number() over (order by cal_date desc) as rn
            from {{ ref('dim_calendar') }}
            where is_business_day = true
              and cal_date <= {{ get_business_date() }}
        ) d
        where rn <= {{ lookback_days }}

    {% else %}

        -- Full refresh / first run
        cast({{ default_start_date }} as date)

    {% endif %}
)

{% endmacro %}
