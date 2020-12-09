{%- macro grant_usage_to_schemas() -%}

    {#
        This works in conjunction with the Permifrost roles.yml file. 
        This will only run on production and mainly covers our bases so that
        new models created will be immediately available for querying to the 
        roles listed.

    #}

    {%- set non_sensitive = 'dbt_analytics' -%}
    {%- set sensitive = 'dbt_analytics_sensitive' -%}
    {%- set clones = 'dbt_analytics_clones' -%}

    {%- if target.name == 'prod' -%}
        grant usage on schema legacy to role {{ non_sensitive }};
        grant select on all tables in schema legacy to role {{ non_sensitive }};
        grant select on all views in schema legacy to role {{ non_sensitive }};

        grant usage on schema analytics_clones to role {{ clones }};
        grant select on all tables in schema analytics_clones to role {{ clones }};
        grant select on all views in schema analytics_clones to role {{ clones }};

        grant usage on schema common to role {{ non_sensitive }};
        grant select on all tables in schema common to role {{ non_sensitive }};
        grant select on all views in schema common to role {{ non_sensitive }};
        
        grant usage on schema common_mapping to role {{ non_sensitive }};
        grant select on all tables in schema common_mapping to role {{ non_sensitive }};
        grant select on all views in schema common_mapping to role {{ non_sensitive }};

        grant usage on schema covid19 to role {{ non_sensitive }};
        grant select on all tables in schema covid19 to role {{ non_sensitive }};
        grant select on all views in schema covid19 to role {{ non_sensitive }};

        grant usage on schema prep.sensitive to role {{ sensitive }};
        grant select on all tables in schema prep.sensitive to role {{ sensitive }};
        grant select on all views in schema prep.sensitive to role {{ sensitive }};

        grant select on table prep.sensitive.bamboohr_id_employee_number_mapping to role lmai;
    {%- endif -%}

{%- endmacro -%} 
