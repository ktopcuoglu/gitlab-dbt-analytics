{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "primary_key"
    })
}}

WITH flattened_data AS ( 
  
    SELECT * 
    FROM {{ ref('prep_usage_data_7_days_flattened') }}

), prep_usage_ping_payload AS (

    SELECT *
    FROM {{ ref('prep_usage_ping_payload') }}
  
), weekly AS (

    SELECT  
      DATE_TRUNC('week', ping_created_at) AS created_week,
      prep_usage_ping_payload.dim_usage_ping_id,
      ping_created_at,
      prep_usage_ping_payload.dim_instance_id,
      host_name,
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
      IFNULL(metric_value,0) AS weekly_metrics_value,
      has_timed_out
    FROM flattened_data
    LEFT JOIN prep_usage_ping_payload
      ON flattened_data.dim_usage_ping_id = prep_usage_ping_payload.dim_usage_ping_id
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY created_week, dim_instance_id, host_name, metrics_path ORDER BY ping_created_at DESC)) = 1

), transformed AS (

    SELECT
      {{ dbt_utils.surrogate_key(['dim_instance_id', 'host_name', 'created_week', 'metrics_path']) }} AS primary_key,
      dim_usage_ping_id,
      dim_instance_id,
      host_name,
      created_week,
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
      SUM(weekly_metrics_value)   AS weekly_metrics_value,
      -- if several records and 1 has not timed out, then display FALSE
      MIN(has_timed_out)          AS has_timed_out
    FROM weekly
    {{ dbt_utils.group_by(n=15)}}

)

{{ dbt_audit(
    cte_ref="transformed",
    created_by="@mpeychet",
    updated_by="@mpeychet",
    created_date="2021-05-04",
    updated_date="2021-05-04"
) }}
