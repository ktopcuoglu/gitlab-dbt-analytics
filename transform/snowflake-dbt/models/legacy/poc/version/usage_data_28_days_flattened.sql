{{ config({
    "materialized": "incremental",
    "unique_key": "instance_path_id"
    })
}}

{% set metric_type = '28_days' %}
{% set json_to_parse = ['analytics_unique_visits', 'counts_monthly', 'usage_activity_by_stage_monthly', 'counts', 'usage_activity_by_stage_monthly', 'redis_hll_counters'] %}

WITH data AS ( 
  
    SELECT * FROM {{ ref('version_usage_data')}}
    {% if is_incremental() %}

      WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})

    {% endif %}

)

, flattened AS (
    {% for column in json_to_parse %}
      (

        SELECT 
          {{ dbt_utils.surrogate_key(['id', 'path']) }}      AS instance_path_id,
          uuid                                               AS instance_id, 
          id                                                 AS ping_id,
          host_id,
          created_at,
          path                                               AS metric_path, 
          value                                              AS metric_value
        FROM data,
        lateral flatten(input => raw_usage_data_payload, path => '{{ column }}',
        recursive => true) 
        WHERE typeof(value) IN ('INTEGER', 'DECIMAL')
        ORDER BY created_at DESC

      )

      {% if not loop.last %}
        UNION 
      {% endif %}
      {% endfor %}

)

SELECT 
  flattened.instance_path_id,
  flattened.instance_id,
  flattened.ping_id,
  host_id,
  created_at,
  flattened.metric_path AS flat_metrics_path,
  metrics.*, 
  IFF(flattened.metric_value = -1, 0, flattened.metric_value) AS metric_value
FROM flattened
INNER JOIN {{ ref('sheetload_usage_ping_metrics_sections' )}} AS metrics 
  ON flattened.metric_path = metrics.metrics_path
    AND time_period = '{{metric_type}}'
