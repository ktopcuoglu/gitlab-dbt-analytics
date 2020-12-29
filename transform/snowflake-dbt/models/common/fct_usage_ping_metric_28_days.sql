{{config({
    "schema": "legacy"
  })
}}

WITH usage_data AS (

    SELECT *
    FROM {{ ref('dim_usage_pings') }}

), unpacked_stages AS (
    
    SELECT
      usage_data.*,
      f.key                                                              AS stage_name,
      f.value                                                            AS stage_activity_count_json

    FROM usage_data,
      lateral flatten(input => usage_data.usage_activity_by_stage_monthly) f

), unpacked_metric_names AS (

    SELECT 
      unpacked_stages.*,
      data.key                 AS metric_name,
      data.value               AS metric_value
    FROM unpacked_stages,
      lateral flatten(input => unpacked_stages.stage_activity_count_json) data

), renamed AS (

    SELECT
      id          AS usage_ping_id,
      recorded_at AS recorded_at,
      stage_name,
      metric_name,
      metric_value
    FROM unpacked_metric_names

)

SELECT *
FROM renamed

