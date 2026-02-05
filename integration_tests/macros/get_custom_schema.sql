{#-
    Custom schema macro that returns the custom_schema directly without
    prepending the default schema. This allows us to put prod models in
    the prod test dataset and compare models in the compare test dataset.
-#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
