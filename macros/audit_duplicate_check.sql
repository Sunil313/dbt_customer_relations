{% macro audit_duplicate_check(model_name, column_list) %}
select
    {{ column_list | join(', ') }},
    count(*) as record_count
from {{ ref(model_name) }}
group by {{ column_list | join(', ') }}
having count(*) > 1
{% endmacro %}
