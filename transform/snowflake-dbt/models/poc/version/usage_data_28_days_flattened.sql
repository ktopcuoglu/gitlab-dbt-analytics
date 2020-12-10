
{% set metric_type = '28_days' %}
{% set json_to_parse = ['analytics_unique_visits', 'counts_monthly', 'usage_activity_by_stage_monthly', 'stats_used', 'usage_activity_by_stage_monthly', 'redis_hll_counters'] %}
{% set columns_to_parse = ['stats_used', 'usage_activity_by_stage_monthly'] %}

WITH data AS ( 
  
    SELECT * FROM {{ ref('version_usage_data')}}

)

, flattened AS (
    {% for column in json_to_parse %}
      (

        SELECT 
          uuid                          AS instance_id, 
          id                            AS ping_id,
          host_id,
          created_at,
          path                          AS metric_path, 
          value                         AS metric_value
        FROM data,
        lateral flatten(input => raw_usage_data_payload, path => '{{ column }}',
        recursive => true) 
        WHERE typeof(value) IN ('INTEGER', 'DECIMAL')
        ORDER BY created_at DESC

      )

      {% if column in columns_to_parse %}
      UNION 

      (

        SELECT 
          uuid                          AS instance_id, 
          id                            AS ping_id,
          host_id,
          created_at,
          '{{column}}' || '.' || path   AS metric_path, 
          value                         AS metric_value
        FROM data,
        lateral flatten(input => {{column}}, 
        recursive => true) 
        WHERE raw_usage_data_payload IS NULL
        ORDER BY created_at DESC

      )
      {% endif %}

      {% if not loop.last %}
        UNION 
      {% endif %}
      {% endfor %}

)

SELECT 
  flattened.instance_id,
  flattened.ping_id,
  host_id,
  created_at,
  flattened.metric_path AS flat_metrics_path,
  metrics.*, 
  flattened.metric_value
FROM flattened
INNER JOIN {{ ref('sheetload_usage_ping_metrics_sections' )}} AS metrics 
  ON flattened.metric_path = metrics.metrics_path
    AND time_period = '{{metric_type}}'
