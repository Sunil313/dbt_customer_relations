{% macro enforce_required_columns(model_name, required_columns) %}

{% set relation = ref(model_name) %}
{% set cols = adapter.get_columns_in_relation(relation) %}
{% set existing_cols = cols | map(attribute='name') | map('lower') | list %}

{% set missing_cols = [] %}

{% for col in required_columns %}
  {% if col | lower not in existing_cols %}
    {% do missing_cols.append(col) %}
  {% endif %}
{% endfor %}

{% if missing_cols | length > 0 %}
  {{ exceptions.raise_compiler_error(
      "Model '" ~ model_name ~ "' is missing required columns: " ~ missing_cols | join(', ')
  ) }}
{% endif %}

{% endmacro %}
