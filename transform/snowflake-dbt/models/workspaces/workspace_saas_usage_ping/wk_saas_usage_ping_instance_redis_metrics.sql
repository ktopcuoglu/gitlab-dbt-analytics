{{ config({
    "materialized": "table"
    })
}}

{{ simple_cte([
    ('instance_redis_metrics', 'instance_redis_metrics'),
    ('dim_usage_ping_metric', 'dim_usage_ping_metric')

]) }}

, flattened AS (

    SELECT
      saas_usage_ping_redis_id                            AS saas_usage_ping_redis_id,
      ping_date                                           AS ping_date,
      COALESCE(TRY_PARSE_JSON(path)[0]::TEXT, path::TEXT) AS metric_path,
      value::TEXT                                         AS metric_value,
      recorded_at                                         AS recorded_at,
      version                                             AS version,
      edition                                             AS edition,
      recording_ce_finished_at                            AS recording_ce_finished_at,
      recording_ee_finished_at                            AS recording_ee_finished_at,
      uuid                                                AS uuid,
      _uploaded_at                                        AS _uploaded_at
    FROM instance_redis_metrics,
    LATERAL FLATTEN(INPUT => response,
    RECURSIVE => TRUE) AS r
    -- -3 is for non redis metrics so we exclude them from the model
    WHERE TRY_TO_NUMBER(value::TEXT) <> -3

), joined AS (

    SELECT
      flattened.saas_usage_ping_redis_id         AS saas_usage_ping_redis_id,
      flattened.ping_date                        AS ping_date,
      flattened.metric_path                      AS metric_path,
      flattened.metric_value                     AS metric_value,
      dim_usage_ping_metric.metrics_status       AS metric_status,
      flattened.recorded_at                      AS recorded_at,
      flattened.version                          AS version,
      flattened.edition                          AS edition,
      flattened.recording_ce_finished_at         AS recording_ce_finished_at,
      flattened.recording_ee_finished_at         AS recording_ee_finished_at,
      flattened.uuid                             AS uuid,
      flattened._uploaded_at                     AS _uploaded_at
    FROM flattened
    LEFT JOIN dim_usage_ping_metric
    ON flattened.metric_path = dim_usage_ping_metric.metrics_path

)
SELECT *
FROM joined

