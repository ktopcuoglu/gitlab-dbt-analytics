{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([('dim_date', 'dim_date'),
                ('prep_usage_ping_payload', 'prep_usage_ping_payload')
                ]
                )}}

, data AS ( 
  
    SELECT * 
    FROM {{ ref('prep_usage_data_all_time_flattened')}}
    WHERE typeof(metric_value) IN ('INTEGER', 'DECIMAL')

), transformed AS (

    SELECT 
        prep_usage_ping_payload.*,
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
        DATE_TRUNC('month', ping_created_at) AS ping_created_month
    FROM data
    LEFT JOIN prep_usage_ping_payload
      ON data.dim_usage_ping_id = prep_usage_ping_payload.dim_usage_ping_id
    -- need host_name in the QUALIFY statement
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_instance_id, metrics_path, ping_created_month ORDER BY ping_created_at DESC) = 1

)

, monthly AS (

    SELECT 
      *,
      LAG(ping_created_at) OVER (
        PARTITION BY dim_instance_id, host_name, metrics_path 
        ORDER BY ping_created_month ASC
      )                                                           AS last_ping_date,
      COALESCE(LAG(metric_value) OVER (
        PARTITION BY dim_instance_id, host_name, metrics_path 
        ORDER BY ping_created_month ASC
      ), 0)                                                       AS last_ping_value,
      DATEDIFF('day', last_ping_date, ping_created_at)            AS days_since_last_ping,
      metric_value - last_ping_value                              AS monthly_metric_value,
      monthly_metric_value * 28 / IFNULL(days_since_last_ping, 1) AS normalized_monthly_metric_value
    FROM transformed

), final AS (

    SELECT
      {{ dbt_utils.surrogate_key(['dim_instance_id', 'host_name', 'ping_created_month', 'metrics_path']) }} AS primary_key,
      dim_usage_ping_id,
      dim_instance_id,
      host_name,
      ping_created_month,
      dim_date.date_id                                       AS month_date_id, 
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
    LEFT  JOIN dim_date
      ON monthly.ping_created_month = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-07-21",
    updated_date="2021-07-21"
) }}
