{{ 
    config({
        "materialized": "incremental",
        "unique_key": "full_metrics_path"
    })
}}

WITH prep_usage_ping AS (
  
    SELECT * 
    FROM {{ ref('prep_usage_ping') }}
    
), usage_ping_full_name AS (

    SELECT DISTINCT 
        TRIM(LOWER(f.path))                                                        AS metrics_path,
        REPLACE(metrics_path, '.','_')                                             AS metrics_path_column_name,
        'raw_usage_data_payload::' || REPLACE(metrics_path, '.','::')              AS full_metrics_path,
        SPLIT_PART(metrics_path, '.', 1)                                           AS main_json_name, 
        SPLIT_PART(metrics_path, '.', -1)                                          AS feature_name
    FROM prep_usage_ping,
    lateral flatten(input => prep_usage_ping.raw_usage_data_payload, recursive => True) f

    {% if is_incremental() %}

        WHERE ping_created_at_date > (SELECT MAX(ping_created_at_date) FROM {{ this }})

    {% endif %}

), final AS (

    SELECT * 
    FROM usage_ping_full_name
    WHERE feature_name NOT IN ('source_ip')

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@kathleentam",
    updated_by="@kathleentam",
    created_date="2021-03-01",
    updated_date="2021-03-01"
) }}

