{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
    
        {{ default_schema }}
    
    {%- else -%}
    
        {# Logic: If the environment is 'prod', use the custom schema directly #}
        {%- if target.name == 'prod' -%}
            {{ custom_schema_name | trim }}
            
        {# Logic: For dev/staging, keep the prefix so developers don't clash #}
        {%- else -%}
            {{ default_schema }}_{{ custom_schema_name | trim }}
        {%- endif -%}
    
    {%- endif -%}

{%- endmacro %}