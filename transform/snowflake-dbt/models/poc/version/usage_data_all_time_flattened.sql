
{% set metric_type = 'all_time' %}
{% set columns_to_parse = ['usage_activity_by_stage_monthly', 'stats_used', 'usage_activity_by_stage_monthly'] %}

WITH data AS ( 
  
    SELECT * FROM {{ ref('version_usage_data')}}

)

, flattened AS (
    {% for column in columns_to_parse %}
    (

      SELECT 
        uuid, 
        id,
        created_at,
        path AS metric_path, 
        '{{ column }}' || '.' || path AS full_metrics_path,
        value AS metric_value
      FROM data,
      lateral flatten(input => {{ column }},
      recursive => true) 
      WHERE typeof(value) IN ('INTEGER', 'DECIMAL')
      ORDER BY created_at DESC

    )
    {% if not loop.last %}UNION {% endif %}
    {% endfor %}

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
    AND time_window = '{{metric_type}}'
