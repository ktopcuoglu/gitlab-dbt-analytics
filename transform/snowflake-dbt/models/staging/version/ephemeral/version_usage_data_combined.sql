WITH unioned AS (

    {{ 
        dbt_utils.union_relations(
            relations=[ref('snowflake_imports_usage_ping_payloads_source'), ref('version_usage_data_source')]
        ) 
    }}

)

SELECT *
FROM unioned