{{ config({
    "materialized": "incremental",
    "unique_key": "primary_key"
    })
}}

WITH data AS ( 
  
    SELECT * 
    FROM {{ ref('prep_usage_data_all_time_flattened')}}
    {% if is_incremental() %}

      WHERE created_at >= (SELECT MAX(DATEADD('month', -1, created_month)) FROM {{this}})

    {% endif %}

)

, transformed AS (

    SELECT 
        *,
        DATE_TRUNC('month', created_at) AS created_month
    FROM data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY instance_id, metrics_path, host_id, created_month ORDER BY created_at DESC) = 1

)

, monthly AS (

    SELECT 
      *,
      metric_value 
        - COALESCE(LAG(metric_value) OVER (
                                            PARTITION BY instance_id, host_id, metrics_path 
                                            ORDER BY created_month
                                          ), 0) AS monthly_metric_value
    FROM transformed

)

SELECT
  {{ dbt_utils.surrogate_key(['instance_id', 'host_id', 'created_month', 'metrics_path']) }} AS primary_key,
  ping_id,
  instance_id,
  host_id,
  created_month,
  metrics_path,
  group_name,
  stage_name,
  section_name,
  is_smau,
  is_gmau,
  is_paid_gmau,
  is_umau,
  clean_metrics_name,
  IFF(monthly_metric_value < 0, 0, monthly_metric_value) AS monthly_metric_value,
  has_timed_out
FROM monthly
  {% if is_incremental() %}

    WHERE created_month >= (SELECT MAX(created_month) FROM {{this}})

  {% endif %}
