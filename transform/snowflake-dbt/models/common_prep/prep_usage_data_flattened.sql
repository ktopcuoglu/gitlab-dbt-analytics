{{ config(
    tags=["product", "mnpi_exception"]
) }}


{{ config({
    "materialized": "incremental",
    "unique_key": "instance_path_id"
    })
}}

WITH data AS ( 
  
    SELECT * FROM {{ ref('prep_usage_ping_payload')}}
    {% if is_incremental() %}

      WHERE dim_date_id >= (SELECT MAX(dim_date_id) FROM {{this}})

    {% endif %}

)

, flattened AS (

    SELECT 
      {{ dbt_utils.surrogate_key(['dim_usage_ping_id', 'path']) }}      AS instance_path_id, 
      dim_usage_ping_id,
      dim_date_id,
      path                                                              AS metrics_path, 
      value                                                             AS metric_value
    FROM data,
    LATERAL FLATTEN(INPUT => raw_usage_data_payload,
    RECURSIVE => TRUE) 

)

{{ dbt_audit(
    cte_ref="flattened",
    created_by="@mpeychet",
    updated_by="@mpeychet",
    created_date="2021-07-21",
    updated_date="2021-07-21"
) }}

