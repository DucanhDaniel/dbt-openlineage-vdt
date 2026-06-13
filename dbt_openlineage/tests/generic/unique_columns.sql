{% test unique_columns(model, columns) %}

    select
        {% for col in columns %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    from {{ model }}
    group by
        {% for col in columns %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    having count(*) > 1

{% endtest %}
