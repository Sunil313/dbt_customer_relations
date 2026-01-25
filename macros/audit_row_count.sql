{% macro audit_row_count(source_relation, target_relation) %}
select
    '{{ source_relation }}' as source_table,
    '{{ target_relation }}' as target_table,
    (select count(*) from {{ source_relation }}) as source_count,
    (select count(*) from {{ target_relation }}) as target_count
{% endmacro %}
