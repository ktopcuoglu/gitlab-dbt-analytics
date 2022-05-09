{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('dim_date','dim_date'),
    ('mart_event_with_valid_user', 'mart_event_with_valid_user')
    ])
}},

mart_with_date_range AS (

  SELECT
    mart_event_with_valid_user.*,
    dim_date.last_day_of_month AS last_day_of_month,
    dim_date.last_day_of_quarter AS last_day_of_quarter,
    dim_date.last_day_of_fiscal_year AS last_day_of_fiscal_year
  FROM mart_event_with_valid_user
  LEFT JOIN dim_date
    ON mart_event_with_valid_user.event_date = dim_date.date_actual
  WHERE mart_event_with_valid_user.event_date BETWEEN DATEADD('day', -27, last_day_of_month) AND last_day_of_month

),

mart_usage_event_plan_monthly AS (

  SELECT
    {{ dbt_utils.surrogate_key(['event_calendar_month', 'plan_id_at_event_date', 'event_name']) }} AS event_plan_monthly_id,
    event_calendar_month,
    event_calendar_quarter,
    event_calendar_year,
    plan_id_at_event_date,
    event_name,
    stage_name,
    section_name,
    group_name,
    is_smau,
    is_gmau,
    is_umau,
    COUNT(*) AS event_count,
    COUNT(DISTINCT(dim_ultimate_parent_namespace_id)) AS ultimate_parent_namespace_count,
    COUNT(DISTINCT(dim_user_id)) AS user_count
  FROM mart_with_date_range
  {{ dbt_utils.group_by(n=12) }}
  ORDER BY event_calendar_month DESC, plan_id_at_event_date DESC

)

{{ dbt_audit(
    cte_ref="mart_usage_event_plan_monthly",
    created_by="@dihle",
    updated_by="@iweeks",
    created_date="2022-02-22",
    updated_date="2022-05-05"
) }}
