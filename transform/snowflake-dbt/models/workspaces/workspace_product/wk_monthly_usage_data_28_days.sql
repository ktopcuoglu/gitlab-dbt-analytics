{{ config({
    "materialized": "incremental",
    "unique_key": "primary_key"
    })
}}

{{ simple_cte([('dim_date', 'dim_date'),
                ('fct_usage_ping_payload', 'fct_usage_ping_payload')
                ]
                )}}

, data AS ( 
  
    SELECT * 
    FROM {{ ref('wk_prep_usage_data_28_days_flattened')}}
    WHERE typeof(metric_value) IN ('INTEGER', 'DECIMAL')

    {% if is_incremental() %}

      AND ping_created_at >= (SELECT MAX(ping_created_month) FROM {{this}})

    {% endif %}

), joined AS (

    SELECT  
      DATE_TRUNC('week', ping_created_at) AS ping_created_week,
      dim_instance_id,
      NULL AS dim_host_id,
      dim_date_id,
      fct_usage_ping_payload.dim_usage_ping_id,
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
    FROM data
    LEFT JOIN fct_usage_ping_payload
      ON data.dim_usage_ping_id = fct_usage_ping_payload.dim_usage_ping_id

), monthly AS (

    SELECT  
      DATE_TRUNC('month', ping_created_week) AS ping_created_month,
      dim_instance_id,
      dim_host_id,
      dim_usage_ping_id,
      dim_date_id,
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
    FROM joined
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY ping_created_month, dim_instance_id, dim_host_id, metrics_path ORDER BY ping_created_week DESC, dim_date_id DESC)) = 1

)

SELECT
  {{ dbt_utils.surrogate_key(['dim_instance_id', 'dim_host_id', 'ping_created_month', 'metrics_path']) }} AS primary_key,
  dim_instance_id,
  dim_host_id,
  dim_usage_ping_id,
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
  SUM(monthly_metric_value)   AS monthly_metric_value,
  SUM(original_metric_value)  AS original_metric_value,
  -- if several records and 1 has not timed out, then display FALSE
  MIN(has_timed_out)        AS has_timed_out
FROM monthly
{{ dbt_utils.group_by(n=15)}}
