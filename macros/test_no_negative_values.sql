{% macro test_positive_values(model, column_name) %}
  select
      count(*) as failures
  from {{ model }}
  where {{ column_name }} <= 0
{% endmacro %}
