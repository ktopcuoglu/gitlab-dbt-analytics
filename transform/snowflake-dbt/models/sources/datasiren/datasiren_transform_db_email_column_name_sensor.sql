{{ config({
        "materialized": "incremental"
    })
}}

{{ datasiren.snowflake__email_column_name_sensor(env_var('SNOWFLAKE_TRANSFORM_DATABASE')) }}