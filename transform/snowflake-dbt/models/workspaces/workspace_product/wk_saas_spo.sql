{%- set stage_names = dbt_utils.get_column_values(ref('wk_prep_stages_to_include'), 'stage_name', default=[]) -%}

{{ config({
    "materialized": "table"
    })
}}

{{simple_cte([
  ('date_details', 'date_details'),
  ('blocked_users', 'gitlab_dotcom_users_blocked_xf'),
  ('all_events', 'gitlab_dotcom_daily_usage_data_events'),
  ('metrics', 'gitlab_dotcom_xmau_metrics')
])
}}

, all_namespaces AS (
  
    SELECT
      namespace_id,
      namespace_type,
      creator_id,
      namespace_created_at
    FROM {{ ref('gitlab_dotcom_namespaces_xf') }}
    WHERE namespace_id = namespace_ultimate_parent_id
      AND namespace_is_internal = FALSE

), namespaces AS (
  
    SELECT 
      all_namespaces.*,
      IFF(blocked_users.user_id IS NOT NULL,TRUE,FALSE) AS created_by_blocked_user
    FROM all_namespaces
    LEFT JOIN blocked_users ON all_namespaces.creator_id = blocked_users.user_id
  
), events AS (
  
    SELECT
      all_events.namespace_id,
      event_date,
      DATE_TRUNC('month', event_date)                             AS event_month,
      plan_name_at_event_date,
      user_id,
      all_events.stage_name,
      IFF(all_events.stage_name='manage',user_id,NULL)            AS umau,
      FIRST_VALUE(plan_name_at_event_date) OVER (
        PARTITION BY event_month, all_events.namespace_id 
        ORDER BY event_date ASC)                                  AS plan_name_at_reporting_month,
      FIRST_VALUE(plan_name_at_event_date) OVER (
        PARTITION BY all_events.namespace_id 
        ORDER BY event_date ASC)                                  AS plan_name_at_creation,
      COUNT(event_date)                                           AS event_count
    FROM all_events
    INNER JOIN metrics ON all_events.event_name = metrics.events_to_include
    WHERE (metrics.smau = TRUE OR metrics.is_umau = TRUE)
      AND all_events.stage_name != 'monitor'
      AND namespace_is_internal = FALSE
      AND days_since_namespace_creation >= 0
    {{dbt_utils.group_by(n=7)}}
                                                                                  
), joined AS (                                               

    SELECT
      'SaaS'                                                                  AS delivery, 
      namespaces.namespace_id                                                 AS organization_id,
      namespace_type                                                          AS organization_type,
      DATE(namespace_created_at)                                              AS organization_creation_date,
      first_day_of_month                                                      AS reporting_month,
      stage_name,
      plan_name_at_reporting_month,
      created_by_blocked_user,
      IFF(plan_name_at_reporting_month IN ('free','trial'), TRUE, FALSE)     AS plan_is_paid,
      SUM(event_count)                                                        AS monthly_stage_events,
      COUNT(DISTINCT user_id)                                                 AS monthly_stage_users,
      COUNT(DISTINCT event_date)                                              AS stage_active_days,
      COUNT(DISTINCT umau)                                                    AS umau_stage,
      SUM(umau_stage) OVER (
        PARTITION BY organization_id, first_day_of_month, plan_name_at_reporting_month
      )                                                                       AS umau
    FROM events
    INNER JOIN date_details 
      ON events.event_month = date_details.date_day
    INNER JOIN namespaces 
      ON namespaces.namespace_id = events.namespace_id
    WHERE event_date >= DATEADD('day',-28, date_details.last_day_of_month)
      AND stage_name != 'manage'
    {{dbt_utils.group_by(n=9)}}
  
)

SELECT
  reporting_month,
  organization_id::VARCHAR AS organization_id,
  delivery,
  organization_type,
  plan_name_at_reporting_month,
  plan_is_paid,
  --organization_creation_date,
  --created_by_blocked_user,
  umau,
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
{{dbt_utils.group_by(n=7)}}
