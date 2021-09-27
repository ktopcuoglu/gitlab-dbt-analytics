{{ config({
    "materialized": "table"
    })
}}

{{ simple_cte([
    ('instance_redis_metrics', 'instance_redis_metrics'),
    ('dim_date', 'dim_date')
]) }}

, flattened AS (

    SELECT
      saas_usage_ping_redis_id                            AS saas_usage_ping_redis_id,
      ping_date                                           AS ping_date,
      COALESCE(TRY_PARSE_JSON(path)[0]::TEXT, path::TEXT) AS metric_path,
      value::TEXT                                         AS metric_value,
      response['recording_ce_finished_at']::DATE          AS recorded_at
    FROM instance_redis_metrics,
    LATERAL FLATTEN(INPUT => response,
    RECURSIVE => TRUE) AS r
    -- -3 is for non redis metrics so we exclude them from the model
    WHERE TRY_TO_NUMBER(value::TEXT) <> -3

)
SELECT *
FROM flattened

