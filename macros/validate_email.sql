{% macro validate_email(email) %}
    regexp_like({{ email }}, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
{% endmacro %}
