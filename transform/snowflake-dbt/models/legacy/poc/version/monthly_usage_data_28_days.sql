{{ config({
    "materialized": "incremental",
    "unique_key": "primary_key"
    })
}}

WITH data AS ( 
  
    SELECT * 
    FROM {{ ref('prep_usage_data_28_days_flattened')}}
    {% if is_incremental() %}

      WHERE created_at >= (SELECT MAX(created_month) FROM {{this}})

    {% endif %}

), transformed AS (

    SELECT  
      DATE_TRUNC('week', created_at) AS created_week,
      ping_id,
      created_at,
      instance_id,
      host_id,
      metrics_path,
      group_name,
      stage_name,
      section_name,
      is_smau,
      is_gmau,
      is_paid_gmau,
      is_umau,
      clean_metrics_name,
      time_period,
      IFNULL(metric_value,0) AS weekly_metrics_value
      IFNULL(metric_value,0) AS weekly_metrics_value,
      has_timed_out
    FROM data

), monthly AS (

    SELECT  
      DATE_TRUNC('month', created_week) AS created_month,
      instance_id,
      host_id,
      ping_id,
      metrics_path,
      group_name,
      stage_name,
      section_name,
      is_smau,
      is_gmau,
      is_paid_gmau,
      is_umau,
      clean_metrics_name,
      time_period,
      weekly_metrics_value              AS monthly_metric_value,
      weekly_metrics_value              AS original_metric_value,
      has_timed_out
    FROM transformed
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY created_month, instance_id, host_id, metrics_path ORDER BY created_week DESC, created_at DESC)) = 1

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
  time_period,
  SUM(monthly_metric_value)   AS monthly_metric_value,
  SUM(original_metric_value)  AS original_metric_value 
  SUM(monthly_metric_value) AS monthly_metric_value,
  -- if several records and 1 has not timed out, then display FALSE
  MIN(has_timed_out)        AS has_timed_out
FROM monthly
{{ dbt_utils.group_by(n=15)}}
