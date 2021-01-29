{{ config({
    "materialized": "table"
    })
}}

WITH prep_usage_ping AS (

    SELECT * 
    FROM {{ ref('prep_usage_ping') }}
    WHERE license_md5 IS NULL 

), usage_pings_no_license AS (

    {{ sales_wave_2_3_metrics() }}
  
)

{{ dbt_audit(
    cte_ref="usage_pings_no_license",
    created_by="@kathleentam",
    updated_by="@kathleentam",
    created_date="2021-01-11",
    updated_date="2021-01-29"
) }}
