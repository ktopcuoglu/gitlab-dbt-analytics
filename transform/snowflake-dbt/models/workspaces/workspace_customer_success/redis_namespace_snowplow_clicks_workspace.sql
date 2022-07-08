{{
  config(
    materialized='table',
    tags=["mnpi_exception"]
  )
}}

WITH redis_clicks AS (
  SELECT
    *
  FROM {{ ref('snowplow_structured_events_all') }}
  WHERE event_action IN (
    'g_analytics_valuestream',
    'action_active_users_project_repo',
    'push_package',
    'ci_templates_unique',
    'p_terraform_state_api_unique_users',
    'i_search_paid'
  )
),

namespaces AS (
  SELECT
    *
  FROM {{ ref('dim_namespace') }}
),

joined AS (
  SELECT
    redis_clicks.event_id,
    redis_clicks.derived_tstamp,
    redis_clicks.event_action,
    redis_clicks.gsc_pseudonymized_user_id,
    redis_clicks.gsc_namespace_id,
    redis_clicks.gsc_project_id,
    redis_clicks.gsc_plan,
    namespaces.ultimate_parent_namespace_id
  FROM redis_clicks
  LEFT JOIN namespaces ON namespaces.dim_namespace_id = redis_clicks.gsc_namespace_id
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mdrussell",
    updated_by="@iweeks",
    created_date="2022-06-06",
    updated_date="2022-06-27"
) }}