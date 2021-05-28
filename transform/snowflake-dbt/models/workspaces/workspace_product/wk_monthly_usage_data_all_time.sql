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
    FROM {{ ref('wk_prep_usage_data_all_time_flattened')}}
    WHERE typeof(metric_value) IN ('INTEGER', 'DECIMAL')

    {% if is_incremental() %}

      AND ping_created_at >= (SELECT MAX(ping_created_month) FROM {{this}})

    {% endif %}

)

, transformed AS (

    SELECT 
        fct_usage_ping_payload.*,
        metrics_path,
        metric_value,
        group_name,
        stage_name,
        section_name,
        is_smau,
        is_gmau,
        is_paid_gmau,
        is_umau,
        clean_metrics_name,
        time_period,
        has_timed_out,
        NULL                                 AS dim_host_id,
        DATE_TRUNC('month', ping_created_at) AS ping_created_month
    FROM data
    LEFT JOIN fct_usage_ping_payload
      ON data.dim_usage_ping_id = fct_usage_ping_payload.dim_usage_ping_id
    -- need dim_host_id in the QUALIFY statement
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_instance_id, metrics_path, ping_created_month ORDER BY ping_created_at DESC) = 1

)

, monthly AS (

    SELECT 
      *,
      LAG(ping_created_at) OVER (
        PARTITION BY dim_instance_id, dim_host_id, metrics_path 
        ORDER BY ping_created_month ASC
      )                                                           AS last_ping_date,
      COALESCE(LAG(metric_value) OVER (
        PARTITION BY dim_instance_id, dim_host_id, metrics_path 
        ORDER BY ping_created_month ASC
      ), 0)                                                       AS last_ping_value,
      DATEDIFF('day', last_ping_date, ping_created_at)            AS days_since_last_ping,
      metric_value - last_ping_value                              AS monthly_metric_value,
      monthly_metric_value * 28 / IFNULL(days_since_last_ping, 1) AS normalized_monthly_metric_value
    FROM transformed

)

SELECT
  {{ dbt_utils.surrogate_key(['dim_instance_id', 'dim_host_id', 'ping_created_month', 'metrics_path']) }} AS primary_key,
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
  IFF(monthly_metric_value < 0, 0, monthly_metric_value) AS monthly_metric_value,
  metric_value                                           AS original_metric_value,
  normalized_monthly_metric_value,
  has_timed_out
FROM monthly
  {% if is_incremental() %}

    WHERE ping_created_month >= (SELECT MAX(ping_created_month) FROM {{this}})

  {% endif %}
