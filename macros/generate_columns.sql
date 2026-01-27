{% macro generate_columns(include_updated_at=true) %}

    {{ get_business_date() }}                 as business_date,
    current_timestamp                         as created_at,

    {% if include_updated_at %}
        current_timestamp                     as updated_at,
    {% endif %}

    '{{ invocation_id }}'                     as dbt_invocation_id,
    cast('{{ run_started_at }}' as timestamp) as dbt_run_started_at,
    '{{ target.name }}'                       as dbt_target_env

{% endmacro %}
