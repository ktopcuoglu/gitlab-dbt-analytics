{{
  config(
    materialized='table',
    tags=["mnpi_exception"]
  )
}}

WITH clicks_filtered AS (
  SELECT
    *
  FROM {{ ref('redis_namespace_snowplow_clicks_workspace') }}
  WHERE event_action IN (
    'action_active_users_project_repo'
  )
),

months AS (
  {{ dbt_utils.date_spine(
       datepart = "month",
       start_date = "cast('2020-01-01' as date)",
       end_date = "current_date + 1"
     ) }}
),

clicks_namespaces_action AS (
  SELECT DISTINCT
    ultimate_parent_namespace_id,
    event_action
  FROM clicks_filtered
),

/* 
Create a table that contains one record for each namespace, event type, and 
month.
*/
clicks_namespaces_action_months AS (
  SELECT
    months.date_month,
    clicks_namespaces_action.ultimate_parent_namespace_id,
    clicks_namespaces_action.event_action
  FROM clicks_namespaces_action
  CROSS JOIN months
),

clicks_namespaces_action_months__events__joined AS (
  SELECT
    clicks_namespaces_action_months.date_month,
    clicks_namespaces_action_months.ultimate_parent_namespace_id,
    clicks_namespaces_action_months.event_action,
    COUNT(DISTINCT clicks_filtered.gsc_pseudonymized_user_id) AS distinct_users
  FROM clicks_namespaces_action_months
  LEFT JOIN clicks_filtered 
    ON clicks_namespaces_action_months.ultimate_parent_namespace_id = clicks_filtered.ultimate_parent_namespace_id
    AND clicks_namespaces_action_months.date_month = DATE_TRUNC('month', clicks_filtered.derived_tstamp)
    AND clicks_namespaces_action_months.event_action = clicks_filtered.event_action
  {{ dbt_utils.group_by(n = 3) }}
)

{{ dbt_audit(
    cte_ref="clicks_namespaces_action_months__events__joined",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-05-24",
    updated_date="2022-06-07"
) }}