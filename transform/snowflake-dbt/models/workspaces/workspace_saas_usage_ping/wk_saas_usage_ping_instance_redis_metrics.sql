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
      COALESCE(TRY_PARSE_JSON(path)[0]::TEXT, path::TEXT)         AS metric_path,
      value::TEXT                                                 AS metric_value,
      response['recording_ce_finished_at'] AS recorded_at
    FROM instance_redis_metrics,
    LATERAL FLATTEN(INPUT => response,
    RECURSIVE => TRUE) AS r

)
SELECT *
FROM flattened

