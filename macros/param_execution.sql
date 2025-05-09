{% macro param_execution()%}
    {% if target.name == 'dev' %}
        where EJERCICIO = 2016
    {% endif %}
{% endmacro %}