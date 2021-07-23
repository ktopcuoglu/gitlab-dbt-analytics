{{ config({
    "materialized": "table"
    })
}}

{{ simple_cte([
    ('saas_usage_ping_instance', 'saas_usage_ping_instance'),
    ('dim_date', 'dim_date')
]) }}

, flattened AS (

    SELECT
      saas_usage_ping_gitlab_dotcom_id,
      ping_date,
      COALESCE(TRY_PARSE_JSON(path)[0]::TEXT, path::TEXT)         AS metric_path,
      value::TEXT                                                 AS metric_value
    FROM saas_usage_ping_instance,
    LATERAL FLATTEN(INPUT => run_results,
    RECURSIVE => TRUE) 

)
SELECT *
FROM flattened

