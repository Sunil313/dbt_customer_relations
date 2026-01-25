{% macro safe_cast(column, datatype) %}
cast({{ column }} as {{ datatype }})
{% endmacro %}
