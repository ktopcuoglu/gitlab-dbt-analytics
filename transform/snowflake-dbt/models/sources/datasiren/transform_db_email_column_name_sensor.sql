{{ config({
        "materialized": "incremental"
    })
}}

{{ datasiren.snowflake__find_columns_with_names_like('email_column_name_sensor', '%email%', env_var('SNOWFLAKE_TRANSFORM_DATABASE')) }}