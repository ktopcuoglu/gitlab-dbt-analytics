-- Will write to custom schemas not on prod
-- Ensures logging package writes to analytics_meta

{% macro generate_database_name(custom_database_name, node) -%}

    {%- set production_targets = ('prod','docs','ci') -%}

    {#
        Definitions:
            - custom_database_name: schema provided via dbt_project.yml or model config
            - target.database: schema provided by the target defined in profiles.yml
            - target.name: name of the target (dev for local development, prod for production, etc.)
        
        This macro is hard to test, but here are some test cases and expected output.
        (custom_database_name, target.name, target.database) = <output>

        (analytics, prod, analytics) = analytics
        (analytics, ci, analytics) = analytics
        (analytics, dev, tmurphy_scratch) = tmurphy_scratch_analytics
        
        (staging, prod, analytics) = analytics_staging
        (staging, ci, analytics) = analytics_staging
        (staging, dev, tmurphy_scratch) = tmurphy_scratch_staging
        
        (zuora, prod, analytics) = zuora
        (zuora, ci, analytics) = zuora
        (zuora, dev, tmurphy_scratch) = tmurphy_scratch_zuora

    #}
    {%- if target.name in production_targets -%}
        
        {%- if custom_database_name is none -%}

            {{ target.database | trim }}

        {%- else -%}
            
            {{ custom_database_name | trim }}

        {%- endif -%}

    {%- else -%}
    
        {%- if custom_database_name is none -%}
            {# Should probably never happen.. #}
            {{ target.database | trim }}

        {%- else -%}
            
            {{ target.database }}_{{ custom_database_name | trim }}

        {%- endif -%}
    
    {%- endif -%}

{%- endmacro %}
