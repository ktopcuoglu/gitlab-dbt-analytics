{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('dim_date','dim_date'),
    ('mart_event_with_valid_user', 'mart_event_with_valid_user')
    ])
}},

mart_raw AS (

  SELECT
    {{ dbt_utils.star(from=ref('mart_event_with_valid_user'), except=["STAGE_NAME"]) }},
    CASE
      WHEN stage_name = 'manage' THEN NULL ELSE stage_name
    END AS stage_name
  FROM mart_event_with_valid_user
  WHERE dim_user_id IS NOT NULL
    AND (is_umau = TRUE 
         OR is_gmau = TRUE 
         OR is_smau = TRUE
        )

),

mart_with_date_range AS (

  SELECT
    mart_raw.*,
    dim_date.last_day_of_month,
    dim_date.last_day_of_quarter,
    dim_date.last_day_of_fiscal_year
  FROM mart_raw
  LEFT JOIN dim_date
    ON mart_raw.event_date = dim_date.date_actual
  WHERE mart_raw.event_date BETWEEN DATEADD('day', -27, last_day_of_month) AND last_day_of_month

),

paid_flag_by_month AS (

  SELECT
    dim_ultimate_parent_namespace_id,
    event_calendar_month,
    plan_was_paid_at_event_date
  FROM mart_with_date_range
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_ultimate_parent_namespace_id, event_calendar_month
      ORDER BY event_created_at DESC) = 1

),

mart_w_paid_deduped AS (

  SELECT
    mart_with_date_range.event_id,
    mart_with_date_range.event_date,
    mart_with_date_range.last_day_of_month,
    mart_with_date_range.last_day_of_quarter,
    mart_with_date_range.last_day_of_fiscal_year,
    mart_with_date_range.dim_user_id,
    mart_with_date_range.event_name,
    mart_with_date_range.data_source,
    mart_with_date_range.dim_ultimate_parent_namespace_id,
    mart_with_date_range.is_umau,
    mart_with_date_range.is_gmau,
    mart_with_date_range.is_smau,
    mart_with_date_range.section_name,
    mart_with_date_range.stage_name,
    mart_with_date_range.group_name,
    mart_with_date_range.event_calendar_month,
    mart_with_date_range.event_calendar_quarter,
    mart_with_date_range.event_calendar_year,
    paid_flag_by_month.plan_was_paid_at_event_date
  FROM mart_with_date_range
  LEFT JOIN paid_flag_by_month
    ON mart_with_date_range.dim_ultimate_parent_namespace_id = paid_flag_by_month.dim_ultimate_parent_namespace_id
      AND mart_with_date_range.event_calendar_month = paid_flag_by_month.event_calendar_month

),

total_results AS (

  SELECT
    event_calendar_month,
    is_umau,
    is_gmau,
    is_smau,
    section_name,
    stage_name,
    group_name,
    'total' AS user_group,
    ARRAY_AGG(DISTINCT event_name) WITHIN GROUP (ORDER BY event_name) AS event_name_array,
    COUNT(*) AS event_count,
    COUNT(DISTINCT(dim_ultimate_parent_namespace_id)) AS ultimate_parent_namespace_count,
    COUNT(DISTINCT(dim_user_id)) AS user_count
  FROM mart_w_paid_deduped
  {{ dbt_utils.group_by(n=8) }}
  ORDER BY event_calendar_month DESC

),

free_results AS (

  SELECT
    event_calendar_month,
    is_umau,
    is_gmau,
    is_smau,
    section_name,
    stage_name,
    group_name,
    'free' AS user_group,
    ARRAY_AGG(DISTINCT event_name) WITHIN GROUP (ORDER BY event_name) AS event_name_array,
    COUNT(*) AS event_count,
    COUNT(DISTINCT(dim_ultimate_parent_namespace_id)) AS ultimate_parent_namespace_count,
    COUNT(DISTINCT(dim_user_id)) AS user_count
  FROM mart_w_paid_deduped
  WHERE plan_was_paid_at_event_date = FALSE
  {{ dbt_utils.group_by(n=8) }}
  ORDER BY event_calendar_month DESC

),

paid_results AS (

  SELECT
    event_calendar_month,
    is_umau,
    is_gmau,
    is_smau,
    section_name,
    stage_name,
    group_name,
    'paid' AS user_group,
    ARRAY_AGG(DISTINCT event_name) WITHIN GROUP (ORDER BY event_name) AS event_name_array,
    COUNT(*) AS event_count,
    COUNT(DISTINCT(dim_ultimate_parent_namespace_id)) AS ultimate_parent_namespace_count,
    COUNT(DISTINCT(dim_user_id)) AS user_count
  FROM mart_w_paid_deduped
  WHERE plan_was_paid_at_event_date = TRUE
  {{ dbt_utils.group_by(n=8) }}
  ORDER BY event_calendar_month DESC

),

results_wo_pk AS (

  SELECT * 
  FROM total_results
  
  UNION ALL
  
  SELECT * 
  FROM free_results
  
  UNION ALL
  
  SELECT * 
  FROM paid_results

),

results AS (

  SELECT
    {{ dbt_utils.surrogate_key(['event_calendar_month', 'user_group', 'section_name', 'stage_name', 'group_name']) }} AS xmau_metric_monthly_id,
    results_wo_pk.*
  FROM results_wo_pk

)

{{ dbt_audit(
    cte_ref="results",
    created_by="@icooper_acp",
    updated_by="@iweeks",
    created_date="2022-02-23",
    updated_date="2022-05-05"
) }}
