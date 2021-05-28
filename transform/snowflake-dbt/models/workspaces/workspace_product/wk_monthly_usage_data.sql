WITH monthly_usage_data_all_time AS (

    SELECT *
    FROM {{ ref('wk_monthly_usage_data_all_time') }}

)

, monthly_usage_data_28_days AS (

    SELECT *
    FROM {{ ref('wk_monthly_usage_data_28_days') }}

)

SELECT 
  primary_key,
  dim_usage_ping_id,
  dim_instance_id,
  dim_host_id,
  ping_created_month,
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
  monthly_metric_value,
  original_metric_value,
  normalized_monthly_metric_value,
  has_timed_out
FROM monthly_usage_data_all_time

UNION 

SELECT
  primary_key,
  dim_usage_ping_id,
  dim_instance_id,
  dim_host_id,
  ping_created_month,
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
  monthly_metric_value,
  original_metric_value,
  monthly_metric_value AS normalized_monthly_metric_value,
  has_timed_out
FROM monthly_usage_data_28_days
