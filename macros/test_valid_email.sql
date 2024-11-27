{% macro test_valid_email(model, column_name) %}
    select
        count(*) as failures
    from {{ model }}
    where not regexp_like({{ column_name }}, r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
{% endmacro %}
