
{{ config({
    "materialized": "incremental",
    "unique_key": "instance_path_id"
    })
}}

WITH data AS ( 
  
    SELECT * FROM {{ ref('version_usage_data')}}
    {% if is_incremental() %}

      WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})

    {% endif %}

)

, flattened AS (

        SELECT 
          {{ dbt_utils.surrogate_key(['id', 'path']) }}      AS instance_path_id,
          uuid                                               AS instance_id, 
          id                                                 AS ping_id,
          edition,
          host_id,
          created_at,
          version,
          major_minor_version,
          major_version,
          minor_version,
          path                                               AS metrics_path, 
          value                                              AS metric_value
        FROM data,
        lateral flatten(input => raw_usage_data_payload,
        recursive => true) 
        WHERE typeof(value) IN ('INTEGER', 'DECIMAL')

)

SELECT *
FROM flattened
