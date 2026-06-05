{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        peakcart_{{ custom_schema_name }}
    {%- endif -%}
{%- endmacro %}