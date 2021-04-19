{{ config({
    "materialized": "incremental",
    "unique_key": "instance_path_id"
    })
}}

{% set metric_type = '28_days' %}

WITH flattened AS ( 
  
    SELECT * FROM {{ ref('prep_usage_data_flattened') }}

), usage_ping_metrics AS (

    SELECT *
    FROM {{ ref('usage_ping_metrics_latest') }}

)


SELECT 
  flattened.instance_path_id,
  flattened.instance_id,
  flattened.ping_id,
  host_id,
  created_at,
  flattened.metrics_path                                        AS flat_metrics_path,
  metrics.*, 
  IFF(flattened.metric_value = -1, 0, flattened.metric_value)   AS metric_value,
  IFF(flattened.metric_value = -1, TRUE, FALSE)                 AS has_timed_out,
  time_frame
FROM flattened
INNER JOIN usage_ping_metrics
  ON flattened.metrics_path = usage_ping_metrics.metrics_path
    AND time_frame = '28d'
LEFT JOIN {{ ref('sheetload_usage_ping_metrics_sections' )}} AS metrics 
  ON flattened.metrics_path = metrics.metrics_path
