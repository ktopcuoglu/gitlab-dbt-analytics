WITH date_details AS (
  
    SELECT *
    FROM {{ ref('date_details') }}
    WHERE last_day_of_month = date_actual
    
), namespace AS (

  SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_historical_daily') }}
  
), lineage AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_lineage_historical_daily') }}

), statistics AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_statistics_historical_monthly') }}

), storage AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_storage_statistics_historical_monthly') }}

)

SELECT
  date_details.date_actual AS snapshot_month,
  namespace.*,
  lineage.ultimate_parent_id,
  lineage.ultimate_parent_plan_id,
  lineage.namespace_is_internal,
  statistics.shared_runners_seconds,
  statistics.shared_runners_seconds_last_reset,
  storage.repository_size,
  storage.lfs_objects_size,
  storage.wiki_size,
  storage.build_artifacts_size,
  storage.storage_size,
  storage.packages_size
FROM namespace
LEFT JOIN lineage
  ON namespace.namespace_id = lineage.namespace_id
  AND namespace.snapshot_day = lineage.snapshot_day
LEFT JOIN statistics
  ON namespace.namespace_id = statistics.namespace_id
  AND namespace.snapshot_day = statistics.snapshot_month
LEFT JOIN storage
  ON namespace.namespace_id = storage.namespace_id
  AND namespace.snapshot_day = storage.snapshot_month
INNER JOIN date_details
  ON date_details.date_actual = namespace.snapshot_day
