{{ config(
    tags=["mnpi_exception"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "primary_key"
    })
}}

{{ simple_cte([('dim_date', 'dim_date'),
                ('prep_usage_ping_payload', 'prep_usage_ping_payload')
                ]
                )}}

, data AS ( 
  
    SELECT * 
    FROM {{ ref('prep_usage_data_28_days_flattened')}}
    WHERE typeof(metric_value) IN ('INTEGER', 'DECIMAL')

    {% if is_incremental() %}

      AND dim_date_id >= (SELECT MAX(month_date_id) FROM {{this}})

    {% endif %}

), joined AS (

    SELECT  
      ping_created_at_week,
      dim_instance_id,
      prep_usage_ping_payload.host_name    AS host_name,
      data.dim_date_id,
      prep_usage_ping_payload.dim_usage_ping_id,
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
    LEFT JOIN prep_usage_ping_payload
      ON data.dim_usage_ping_id = prep_usage_ping_payload.dim_usage_ping_id

), monthly AS (

    SELECT  
      DATE_TRUNC('month', ping_created_at_week) AS ping_created_month,
      dim_date.date_id                          AS month_date_id, 
      dim_instance_id,
      host_name,
      dim_usage_ping_id,
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
      weekly_metrics_value                     AS monthly_metric_value,
      weekly_metrics_value                     AS original_metric_value,
      has_timed_out
    FROM joined
    LEFT JOIN dim_date
      ON DATE_TRUNC('month', ping_created_at_week) = dim_date.date_day
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY ping_created_month, dim_instance_id, host_name, metrics_path ORDER BY ping_created_at_week DESC, joined.dim_date_id DESC)) = 1

), final AS (

    SELECT
      {{ dbt_utils.surrogate_key(['dim_instance_id', 'host_name', 'ping_created_month', 'metrics_path']) }} AS primary_key,
      dim_instance_id,
      host_name,
      dim_usage_ping_id,
      ping_created_month,
      month_date_id,
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
      MIN(has_timed_out)          AS has_timed_out
    FROM monthly
    {{ dbt_utils.group_by(n=16)}}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-07-21",
    updated_date="2021-07-21"
) }}

