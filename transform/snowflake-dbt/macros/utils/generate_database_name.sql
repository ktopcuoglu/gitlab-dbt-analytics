-- Will write to custom databases not on prod

{% macro generate_database_name(custom_database_name, node) -%}

    {%- set production_targets = production_targets() -%}

    {#
        Definitions:
            - custom_database_name: database provided via dbt_project.yml or model config
            - target.name: name of the target (dev for local development, prod for production, etc.)
            - target.database: database provided by the target defined in profiles.yml
        
        Assumptions:
            - dbt users will have USERNAME_PROD, USERNAME_PREP DBs defined

        This macro is hard to test, but here are some test cases and expected output.
        (custom_database_name, target.name, target.database) = <output>

        (analytics, prod, analytics) = analytics
        (analytics, ci, analytics) = analytics
        (analytics, dev, tmurphy) = tmurphy_analytics
        
        (prod, prod, analytics) = prod
        (prod, ci, analytics) = prod
        (prod, dev, tmurphy) = tmurphy_prod
        
        (prep, prod, analytics) = prep
        (prep, ci, analytics) = prep
        (prep, dev, tmurphy) = tmurphy_prep

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
