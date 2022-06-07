{{
  config(
    materialized='table',
    tags=["mnpi_exception"]
  )
}}

WITH filtered_events AS (
  SELECT
    *
  FROM {{ ref('snowplow_structured_events_all') }}
  WHERE event_action IN (
    'action_active_users_project_repo'
  )
),

namespaces AS (
  SELECT
    *
  FROM {{ ref('dim_namespace') }}
),

months AS (
  {{ dbt_utils.date_spine(
       datepart = "month",
       start_date = "cast('2020-01-01' as date)",
       end_date = "current_date + 1"
     ) }}
),

event_namespaces_action AS (
  SELECT DISTINCT
    gsc_namespace_id,
    event_action
  FROM filtered_events
),

/* 
Create a table that contains one record for each namespace, event type, and 
month.
*/
event_namespaces_action_months AS (
  SELECT
    months.date_month,
    event_namespaces_action.gsc_namespace_id,
    event_namespaces_action.event_action
  FROM event_namespaces_action
  CROSS JOIN months
),

event_namespaces_action_months__events__joined AS (
  SELECT
    event_namespaces_action_months.date_month,
    namespaces.ultimate_parent_namespace_id,
    event_namespaces_action_months.event_action,
    COUNT(DISTINCT filtered_events.gsc_pseudonymized_user_id) AS distinct_users
  FROM event_namespaces_action_months
  LEFT JOIN namespaces
    ON event_namespaces_action_months.gsc_namespace_id = namespaces.dim_namespace_id
  LEFT JOIN filtered_events 
    ON event_namespaces_action_months.gsc_namespace_id = filtered_events.gsc_namespace_id
    AND event_namespaces_action_months.date_month = DATE_TRUNC('month', filtered_events.derived_tstamp)
    AND event_namespaces_action_months.event_action = filtered_events.event_action
  {{ dbt_utils.group_by(n = 3) }}
)

{{ dbt_audit(
    cte_ref="event_namespaces_action_months__events__joined",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-05-24",
    updated_date="2022-05-26"
) }}