{% macro incremental_filter(date_column) %}
{% if is_incremental() %}
    {{ date_column }} >= {{ max_loaded_date(this.name, date_column) }}
{% else %}
    1 = 1
{% endif %}
{% endmacro %}
