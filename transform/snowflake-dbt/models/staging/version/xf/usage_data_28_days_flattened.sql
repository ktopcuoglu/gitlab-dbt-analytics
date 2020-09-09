WITH data AS ( 
  
    SELECT * FROM {{ ref('version_usage_data')}}

)

, flattened AS (

    SELECT 
      uuid, 
      id, 
      created_at,
      path AS metric_path, 
      'stats_used.' || path AS full_metrics_path,
      value AS metric_value
    FROM data,
    lateral flatten(input => usage_activity_by_stage_monthly,
    recursive => true) X
    WHERE typeof(value) IN ('INTEGER', 'DECIMAL')
    ORDER BY created_at DESC

)

SELECT 
  flattened.uuid,
  flattened.id,
  created_at,
  test.*, 
  flattened.metric_value
FROM flattened
INNER JOIN {{ ref('test_metrics_renaming')}} AS test 
  ON flattened.full_metrics_path = test.full_metrics_path
    AND time_window = '28_days'
