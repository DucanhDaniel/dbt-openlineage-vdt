{% test not_null_columns(model, columns) %}

    select *
    from {{ model }}
    where
        {% for col in columns %}
            {{ col }} is null {% if not loop.last %} or {% endif %}
        {% endfor %}

{% endtest %}
