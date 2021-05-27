{{ config({
    "materialized": "incremental",
    "unique_key": "instance_path_id"
    })
}}

WITH flattened AS ( 
  
    SELECT * FROM {{ ref('wk_prep_usage_data_flattened') }}

), usage_ping_metrics AS (

    SELECT * FROM {{ ref('dim_usage_ping_metric') }}

), joined AS (

    SELECT 
      flattened.dim_instance_path_id,
      flattened.dim_usage_ping_id,
      flattened.metrics_path,
      metrics.section_name,
      metrics.stage_name,
      metrics.group_name,
      COALESCE(metrics.is_smau, FALSE)                              AS is_smau,
      COALESCE(metrics.is_gmau, FALSE)                              AS is_gmau,
      metrics.clean_metrics_name,
      metrics.periscope_metrics_name,
      metrics.time_period,
      COALESCE(metrics.is_umau, FALSE)                              AS is_umau,
      COALESCE(metrics.is_paid_gmau, FALSE)                         AS is_paid_gmau,
      IFF(flattened.metric_value = -1, 0, flattened.metric_value)   AS metric_value,
      IFF(flattened.metric_value = -1, TRUE, FALSE)                 AS has_timed_out,
      time_frame
    FROM flattened
    INNER JOIN usage_ping_metrics
    ON flattened.metrics_path = usage_ping_metrics.metrics_path
        AND time_frame = '7d'
    LEFT JOIN {{ ref('sheetload_usage_ping_metrics_sections' )}} AS metrics 
    ON flattened.metrics_path = metrics.metrics_path

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet",
    updated_by="@mpeychet",
    created_date="2021-05-04",
    updated_date="2021-05-04"
) }}
