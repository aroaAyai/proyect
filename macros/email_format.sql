{% test email_format(model, column_name) %}
    select {{ column_name }}
    from {{ model }}
    where not {{ validate_email(column_name) }}
{% endtest %}
