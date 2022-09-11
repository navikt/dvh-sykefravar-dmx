{% macro dbt_test_source(source, table_name) %}
  {% if target.name == "dbt_test" %}
  {{ ref("mock_"+source+"__"+table_name) }}
  {% else %}
  {{ source(source, table_name) }}
  {% endif %}
{% endmacro %}
