WITH version_usage_data AS (

  SELECT * 
  FROM {{ ref('version_usage_data') }}

)

SELECT DISTINCT
  f.path                     AS ping_name, 
  REPLACE(f.path, '.','_')   AS full_ping_name
FROM version_usage_data,
  lateral flatten(input => version_usage_data.stats_used, recursive => True) f
WHERE IS_OBJECT(f.value) = FALSE
  AND full_ping_name NOT IN ('groups_bugdb_active', 'license_scanning_jobs', 'groups_shimo_active','projects_inheriting_shimo_active', 'projects_inheriting_bugdb_active')