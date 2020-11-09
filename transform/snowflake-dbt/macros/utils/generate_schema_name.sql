-- Will write to custom schemas not on prod
-- Ensures logging package writes to analytics_meta

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set production_targets = production_targets() -%}

    {%- set prefixed_schemas = ('meta','sensitive','staging') -%}

    {#
        Definitions:
            - custom_schema_name: schema provided via dbt_project.yml or model config
            - target.schema: schema provided by the target defined in profiles.yml
            - target.name: name of the target (dev for local development, prod for production, etc.)
        
        This macro is hard to test, but here are some test cases and expected output.
        (custom_schema_name, target.name, target.schema) = <output>

        (analytics, prod, analytics) = analytics
        (analytics, ci, analytics) = analytics
        (analytics, dev, tmurphy_scratch) = analytics
        
        (staging, prod, analytics) = analytics_staging
        (staging, ci, analytics) = analytics_staging
        (staging, dev, tmurphy_scratch) = analytics_staging
        
        (zuora, prod, analytics) = zuora
        (zuora, ci, analytics) = zuora
        (zuora, dev, tmurphy_scratch) = zuora

    #}

    {%- if custom_schema_name in prefixed_schemas -%}

        analytics_{{ custom_schema_name | trim }}

    {%- elif custom_schema_name is none -%}

        {{ target.schema.lower() | trim }}

    {%- else -%}

        {{ custom_schema_name.lower() | trim }}

    {%- endif -%}
    
{%- endmacro %}
