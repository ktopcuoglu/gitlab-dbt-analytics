{{ config({
    "materialized": "incremental",
    "unique_key": "ping_name"
    })
}}
WITH version_usage_data AS (

  SELECT *
  FROM {{ ref('version_usage_data') }}

)

SELECT DISTINCT
  stats.path AS ping_name,
  REPLACE(stats.path, '.', '_') AS full_ping_name
FROM version_usage_data
INNER JOIN LATERAL FLATTEN(input => version_usage_data.stats_used, recursive => TRUE) AS stats
WHERE IS_OBJECT(stats.value) = FALSE
{% if is_incremental() %}
  -- This prevents new metrics, and therefore columns, from being added to downstream tables.
  AND full_ping_name IN (SELECT full_ping_name FROM {{ this }})
{% endif %}
