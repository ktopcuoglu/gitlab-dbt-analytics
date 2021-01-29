{{ 
    config({
        "materialized": "incremental",
        "unique_key": "dim_usage_ping_id"
    })
}}

WITH prep_usage_ping AS (

    SELECT * 
    FROM {{ ref('prep_usage_ping') }}
    WHERE license_md5 IS NULL 

), final AS (

    {{ sales_wave_2_3_metrics() }}
  
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@kathleentam",
    updated_by="@kathleentam",
    created_date="2021-01-11",
    updated_date="2021-01-29"
) }}
