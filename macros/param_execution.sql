{% macro param_execution()%}
    {% if target.name == 'dev' %}
        where EJERCICIO = 2015
    {% endif %}
{% endmacro %}