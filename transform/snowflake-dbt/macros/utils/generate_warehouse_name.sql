-- Will write to custom databases not on prod

{% macro generate_warehouse_name(custom_warehouse_size, node) -%}

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

        
        (prod, prod, prep) = prod
        (prod, ci, prep) = prod
        (prod, dev, tmurphy) = tmurphy_prod
        
        (prep, prod, prep) = prep
        (prep, ci, prep) = prep
        (prep, dev, tmurphy) = tmurphy_prep

    #}
    {%- if target.name in ('prod','docs') -%}
        
        {%- if custom_warehouse_size is none -%}

            {{ target.warehouse }} 

        {%- else -%}
            
            TRANSFORMING_{{ custom_warehouse_size | trim }}

        {%- endif -%}

    {%- else -%}
    
        {%- if custom_warehouse_size is none -%}
            {# Should probably never happen.. #}
            {{ target.warehouse }}

        {%- else -%}
            
            DEV_{{ custom_warehouse_size | trim }}

        {%- endif -%}
    
    {%- endif -%}

{%- endmacro %}
