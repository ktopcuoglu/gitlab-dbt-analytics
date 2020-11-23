WITH data AS ( 
  
    SELECT * 
    FROM {{ ref('usage_data_28_days_flattened')}}

), transformed AS (

    SELECT  
      ping_id,
      DATE_TRUNC('week', created_at) AS created_week,
      instance_id,
      metrics_path,
      group_name,
      stage_name,
      section_name,
      is_smau,
      is_gmau,
      is_paid_gmau,
      is_umau,
      clean_metrics_name,
      SUM(IFNULL(metric_value,0)) AS weekly_metrics_value
    FROM data
    {{ dbt_utils.group_by(n=12) }} 

), monthly AS (

    SELECT  
      ping_id,
      DATE_TRUNC('month', created_week) AS created_month,
      instance_id,
      metrics_path,
      group_name,
      stage_name,
      section_name,
      is_smau,
      is_gmau,
      is_paid_gmau,
      is_umau,
      clean_metrics_name,
      weekly_metrics_value              AS monthly_metric_value
    FROM transformed
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY created_month, instance_id, metrics_path ORDER BY created_week DESC)) = 1

)

SELECT 
  ping_id,
  instance_id,
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
  monthly_metric_value
FROM monthly
