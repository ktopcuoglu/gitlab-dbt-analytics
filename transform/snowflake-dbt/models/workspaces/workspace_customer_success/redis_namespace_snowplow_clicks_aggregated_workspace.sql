{{
  config(
    materialized='table',
    tags=["mnpi_exception", "product"]
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

final AS (
  SELECT
    DATE_TRUNC('month', derived_tstamp) AS date_month,
    ultimate_parent_namespace_id,
    event_action,
    COUNT(DISTINCT gsc_pseudonymized_user_id) AS distinct_users
  FROM clicks_filtered
  {{ dbt_utils.group_by(n = 3) }}
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-05-24",
    updated_date="2022-06-15"
) }}