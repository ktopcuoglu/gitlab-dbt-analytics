{{ config(
    tags=["product", "mnpi_exception"]
) }}

WITH unioned AS (

    SELECT 
      primary_key,
      dim_usage_ping_id,
      dim_instance_id,
      host_name,
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
    FROM {{ ref('prep_monthly_usage_data_all_time') }}

    UNION ALL

    SELECT
      primary_key,
      dim_usage_ping_id,
      dim_instance_id,
      host_name,
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
    FROM {{ ref('prep_monthly_usage_data_28_days') }}

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-07-21",
    updated_date="2021-07-21"
) }}
