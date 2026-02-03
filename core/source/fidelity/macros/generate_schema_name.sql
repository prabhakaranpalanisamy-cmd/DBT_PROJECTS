{% macro generate_schema_name(custom_schema_name, node) %}
    {# If the model has a custom schema, use it alone #}
    {% if custom_schema_name is not none %}
        {{ custom_schema_name }}
    {% else %}
        {{ target.schema }}
    {% endif %}
{% endmacro %}
