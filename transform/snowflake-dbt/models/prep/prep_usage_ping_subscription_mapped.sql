{{ 
    config({
        "materialized": "incremental",
        "unique_key": "dim_usage_ping_id"
    })
}}

WITH usage_pings_with_license_md5 AS (

    SELECT * 
    FROM {{ ref('prep_usage_ping') }}
    WHERE license_md5 IS NOT NULL 

), map_license_subscription_account AS (
  
    SELECT * 
    FROM  {{ ref('map_license_subscription_account') }} 
  
), usage_ping_mapped_to_subscription AS (

    SELECT 
      usage_pings_with_license_md5.*, 
      license_mapped_to_subscription.dim_license_id,
      license_mapped_to_subscription.license_md5,
      license_mapped_to_subscription.is_license_mapped_to_subscription,
      license_mapped_to_subscription.is_license_subscription_id_valid,
      license_mapped_to_subscription.dim_crm_account_id,
      license_mapped_to_subscription.ultimate_parent_account_id,
      IFF(license_mapped_to_subscription.dim_license_id IS NULL, FALSE, TRUE)   AS is_usage_ping_license_in_licenseDot
    FROM usage_pings_with_license_md5
    LEFT JOIN license_mapped_to_subscription
      ON usage_pings_with_license_md5.license_md5 = license_mapped_to_subscription.dim_licenses_license_md5
  
)

{{ dbt_audit(
    cte_ref="usage_ping_mapped_to_subscription",
    created_by="@kathleentam",
    updated_by="@kathleentam",
    created_date="2021-01-11",
    updated_date="2021-01-29"
) }}

