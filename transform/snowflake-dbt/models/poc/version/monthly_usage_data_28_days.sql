WITH data AS ( 
  
    SELECT * 
    FROM {{ ref('usage_data_28_days_flattened')}}

)

, transformed AS (

    SELECT 
      *,
      DATE_TRUNC('month', created_at) AS created_month
    FROM data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY instance_id, clean_metrics_name, created_month ORDER BY created_at DESC) = 1

)

SELECT 
  ping_id,
  instance_id,
  created_month,
  metrics_path,
  stage_name,
  section_name,
  is_smau,
  clean_metrics_name,
  metric_value AS monthly_metric_value
FROM transformed
