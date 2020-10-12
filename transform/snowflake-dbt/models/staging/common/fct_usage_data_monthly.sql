WITH twenty_eight_day_metrics AS (

    SELECT *
    FROM {{ ref('fct_usage_ping_metric_28_days') }}

), all_time_metrics AS (

    SELECT 
      metrics.metric_name,
      metrics.metric_value,
      metrics.usage_ping_id,
      metrics.recorded_at,
      metrics.stage_name,
      payloads.uuid,
      payloads.created_at,
      DATE_TRUNC('month', created_at) AS created_month
    FROM {{ ref('fct_usage_ping_metric_all_time') }} metrics
    INNER JOIN {{ ref('fct_usage_ping_payloads') }} payloads
      ON metrics.usage_ping_id = payloads.usage_ping_id
    WHERE IS_REAL(metric_value) = True
    QUALIFY ROW_NUMBER() OVER (PARTITION BY uuid, metric_name, created_month ORDER BY created_at DESC) = 1

), monthly_all_time_metrics AS (

    SELECT
      *,
      metric_value - COALESCE(
        LAG(metric_value) OVER (
          PARTITION BY uuid, metric_name 
          ORDER BY created_month
        ), 
        0
      ) AS monthly_metric_value
    FROM all_time_metrics

), unioned AS (

    SELECT
      usage_ping_id,
      recorded_at,
      stage_name,
      metric_name,
      IFF(monthly_metric_value < 0, 0, monthly_metric_value)::FLOAT AS metric_value
    FROM monthly_all_time_metrics

    UNION ALL

    SELECT
      usage_ping_id,
      recorded_at,
      stage_name,
      metric_name,
      metric_value::FLOAT AS metric_value
    FROM twenty_eight_day_metrics
    WHERE IS_REAL(metric_value) = True


)

SELECT *
FROM unioned
