{{ config(materialized='view') }}

WITH date_details AS (
  
    SELECT *
    FROM {{ ref('date_details') }}
    WHERE last_day_of_month = date_actual
     
), namespace_snapshots_daily AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_historical_daily') }}
  
), namespace_snapshots_monthly AS (
  
    SELECT
      date_details.first_day_of_month AS snapshot_month,
      namespace_snapshots_daily.namespace_id,
      namespace_snapshots_daily.parent_id,
      namespace_snapshots_daily.owner_id,
      namespace_snapshots_daily.namespace_type,
      namespace_snapshots_daily.visibility_level,
      namespace_snapshots_daily.shared_runners_minutes_limit,
      namespace_snapshots_daily.extra_shared_runners_minutes_limit
    FROM namespace_snapshots_daily
    INNER JOIN date_details
      ON date_details.date_actual = namespace_snapshots_daily.snapshot_day
  
)

SELECT *
FROM namespace_snapshots_monthly
