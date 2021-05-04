{{ config({
    "materialized": "incremental",
    "unique_key": "primary_key"
    })
}}

WITH data AS ( 
  
    SELECT * 
    FROM {{ ref('prep_usage_data_7_days_flattened')}}
    {% if is_incremental() %}

      WHERE created_at >= (SELECT MAX(created_week) FROM {{this}})

    {% endif %}

), weekly AS (

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
      IFNULL(metric_value,0) AS weekly_metrics_value,
      has_timed_out
    FROM data
    QUALIFY (ROW_NUMBER() OVER (PARTITION BY created_week, instance_id, host_id, metrics_path ORDER BY created_week DESC, created_at DESC)) = 1

), transformed AS (

    SELECT
      {{ dbt_utils.surrogate_key(['instance_id', 'host_id', 'created_week', 'metrics_path']) }} AS primary_key,
      ping_id,
      instance_id,
      host_id,
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
