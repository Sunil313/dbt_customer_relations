{% macro audit_custom_dbt(model_relation, row_count=none) %}

    insert into {{ ref('dbt_audit_log') }}
    (
        model_name,
        database_name,
        schema_name,
        business_date,
        run_started_at,
        invocation_id,
        target_name,
        row_count
    )
    values
    (
        '{{ model_relation.identifier }}',
        '{{ model_relation.database }}',
        '{{ model_relation.schema }}',
        {{ get_business_date() }},
        cast('{{ run_started_at }}' as timestamp),
        '{{ invocation_id }}',
        '{{ target.name }}',
        {% if row_count is not none %}
            {{ row_count }}
        {% else %}
            null
        {% endif %}
    );

{% endmacro %}
