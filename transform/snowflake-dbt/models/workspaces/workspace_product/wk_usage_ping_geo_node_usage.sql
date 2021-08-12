{{ config({
    "materialized": "incremental",
    "unique_key": "instance_path_id"
    })
}}

WITH prep_usage_data_flattened AS ( 
  
    SELECT * FROM {{ ref('poc_prep_usage_data_flattened')}}
    {% if is_incremental() %}

      WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})

    {% endif %}

)


, data AS (

    SELECT *
    FROM prep_usage_data_flattened
    WHERE metrics_path = 'usage_activity_by_stage_monthly.enablement.geo_node_usage'
        AND metric_value <> '[]'

), flattened AS (

SELECT *
FROM data,
lateral flatten(input => metric_value,
recursive => true) 

)

SELECT 
  {{ dbt_utils.surrogate_key(['ping_id', 'path']) }}        AS instance_path_id,
  instance_id, 
  ping_id,
  edition,
  host_id,
  created_at,
  version,
  major_minor_version,
  major_version,
  minor_version,
  regexp_replace(split_part(path, '.', 1), '(\\[|\\])', '') AS node_id,
  value AS metric_value,
SPLIT_PART(path, '.', -1) AS metrics_path
FROM flattened
WHERE index IS NULL
