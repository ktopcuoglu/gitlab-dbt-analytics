{{ config({
    "materialized": "table"
    })
}}

WITH usage_pings_with_license_md5 AS (

    SELECT * 
    FROM {{ ref('prep_usage_ping') }}
    WHERE license_md5 IS NOT NULL 

), map_license_subscription_account AS (
  
    SELECT * 
    FROM  {{ ref('map_license_subscription_account') }} 
  
), usage_pings_with_license_md5 AS (

    {{ sales_wave_2_3_metrics() }}
  
), usage_ping_mapped_to_subscription AS (

    SELECT 
      usage_pings_with_license_md5.*, 
      license_mapped_to_subscription.license_user_count, 
      license_mapped_to_subscription.subscription_id, 
      license_mapped_to_subscription.subscription_name, 
      license_mapped_to_subscription.subscription_status,
      license_mapped_to_subscription.is_license_mapped_to_subscription,
      IFF(dim_licenses_license_md5 IS NULL, FALSE, TRUE)    AS is_license_md5_missing_in_licenseDot 
    FROM usage_pings_with_license_md5
    LEFT JOIN license_mapped_to_subscription
      ON usage_pings_with_license_md5.license_md5 = license_mapped_to_subscription.dim_licenses_license_md5
  
)

{{ dbt_audit(
    cte_ref="usage_ping_mapped_to_subscription",
    created_by="@kathleentam",
    updated_by="@kathleentam",
    created_date="2021-01-29",
    updated_date="2021-01-29"
) }}

