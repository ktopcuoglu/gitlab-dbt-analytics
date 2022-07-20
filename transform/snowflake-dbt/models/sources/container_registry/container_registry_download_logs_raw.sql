{{ config(
    materialized='view',
    database=env_var('SNOWFLAKE_PREP_DATABASE'),
    schema='container_registry'
) }}

{{ schema_union_limit('container_registry', 'joined_%', 'timestamp', 90, database_name=env_var('SNOWFLAKE_LOAD_DATABASE')) }}
