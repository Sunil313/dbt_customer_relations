{% macro max_loaded_date(model_name, column_name) %}
(
    select coalesce(max({{ column_name }}), cast('1900-01-01' as date))
    from {{ ref(model_name) }}
)
{% endmacro %}
