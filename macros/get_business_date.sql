{% macro get_business_date(timezone='UTC', cutoff_hour=0) %}

    {% if target.type == 'snowflake' %}
        (
            case
                when extract(hour from convert_timezone('UTC', '{{ timezone }}', current_timestamp())) < {{ cutoff_hour }}
                then dateadd(day, -1, current_date('{{ timezone }}'))
                else current_date('{{ timezone }}')
            end
        )

    {% elif target.type == 'bigquery' %}
        (
            case
                when extract(hour from current_datetime('{{ timezone }}')) < {{ cutoff_hour }}
                then date_sub(current_date('{{ timezone }}'), interval 1 day)
                else current_date('{{ timezone }}')
            end
        )

    {% elif target.type in ['redshift', 'postgres'] %}
        (
            case
                when extract(hour from (current_timestamp at time zone '{{ timezone }}')) < {{ cutoff_hour }}
                then (current_date at time zone '{{ timezone }}') - interval '1 day'
                else current_date at time zone '{{ timezone }}'
            end
        )

    {% else %}
        current_date
    {% endif %}

{% endmacro %}
