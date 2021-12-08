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
-- error when 'license_scanning_jobs' pops up.
-- more details in the issue https://gitlab.com/gitlab-data/analytics/-/issues/10749
AND full_ping_name != 'license_scanning_jobs'