{% macro param_execution()%}
    {% if target.name == 'dev' %}
        where EJERCICIO = 2024
    {% endif %}
{% endmacro %}