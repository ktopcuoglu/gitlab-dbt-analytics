{%- set stage_names = dbt_utils.get_column_values(ref('prep_stages_to_include_spo'), 'stage_name', default=[]) -%}

{{ config({
    "materialized": "table"
    })
}}

{{simple_cte([
  ('dim_namespace', 'dim_namespace'),
  ('prep_gitlab_dotcom_plan', 'prep_gitlab_dotcom_plan'),
  ('dim_date', 'dim_date'),
  ('all_events', 'fct_daily_event_400'),
  ('metrics', 'map_saas_event_to_smau')
])
}}

, events AS (
  
    SELECT
      all_events.ultimate_parent_namespace_id,
      all_events.event_created_date,
      DATE_TRUNC('month', event_created_date)                             AS event_month,
      prep_gitlab_dotcom_plan.plan_name                                   AS plan_name_at_reporting_month,
      dim_user_id,
      metrics.stage_name,
      COUNT(event_created_date)                                           AS event_count,
      0 AS umau
    FROM all_events
    INNER JOIN metrics ON all_events.event_name = metrics.event_name
      AND is_smau
    LEFT JOIN prep_gitlab_dotcom_plan ON all_events.dim_plan_id_at_event_date = prep_gitlab_dotcom_plan.dim_plan_id
    WHERE namespace_is_internal = FALSE
      AND days_since_namespace_creation >= 0
    {{dbt_utils.group_by(n=6)}}
                                                                                  
), joined AS (                                               

    SELECT
      'SaaS'                                                                  AS delivery, 
      events.ultimate_parent_namespace_id                                     AS organization_id,
      namespace_type                                                          AS organization_type,
      DATE(created_at)                                                        AS organization_creation_date,
      first_day_of_month                                                      AS reporting_month,
      stage_name,
      plan_name_at_reporting_month,
      IFF(plan_name_at_reporting_month NOT IN ('free','trial'), TRUE, FALSE)  AS plan_is_paid,
      SUM(event_count)                                                        AS monthly_stage_events,
      COUNT(DISTINCT dim_user_id)                                             AS monthly_stage_users,
      COUNT(DISTINCT event_created_date)                                      AS stage_active_days,
      IFF(monthly_stage_users > 0, TRUE, FALSE)                               AS is_active_stage
    FROM events
    INNER JOIN dim_date 
      ON events.event_month = dim_date.date_day
    INNER JOIN dim_namespace 
      ON dim_namespace.dim_namespace_id = events.ultimate_parent_namespace_id
    WHERE event_created_date >= DATEADD('day',-28, dim_date.last_day_of_month)
    {{dbt_utils.group_by(n=7)}}
  
)

SELECT
  reporting_month,
  organization_id::VARCHAR             AS organization_id,
  delivery,
  organization_type,
  plan_name_at_reporting_month         AS product_tier,
  plan_is_paid                         AS is_paid_product_tier,
  --organization_creation_date,
  --created_by_blocked_user,
  SUM(IFF(is_active_stage > 0 , 1, 0)) AS active_stage_count,
  {{ dbt_utils.pivot(
  'stage_name', 
  stage_names,
  agg = 'MAX',
  then_value = 'monthly_stage_users',
  else_value = 'NULL',
  suffix='_stage',
  quote_identifiers = False
  ) }}
FROM joined
{{dbt_utils.group_by(n=6)}}
