
{{ config({
    "materialized": "incremental",
    "unique_key": "dim_instance_path_id"
    })
}}

WITH data AS ( 
  
    SELECT * FROM {{ ref('fct_usage_ping_payload')}}
    {% if is_incremental() %}

      WHERE dim_date_id >= (SELECT MAX(dim_date_id) FROM {{this}})

    {% endif %}

)

, flattened AS (

        SELECT 
          {{ dbt_utils.surrogate_key(['dim_usage_ping_id', 'path']) }}      AS dim_instance_path_id, 
          dim_usage_ping_id,
          dim_date_id,
          path                                                              AS metrics_path, 
          value                                                             AS metric_value
        FROM data,
        lateral flatten(input => raw_usage_data_payload,
        recursive => true) 

)

SELECT *
FROM flattened
