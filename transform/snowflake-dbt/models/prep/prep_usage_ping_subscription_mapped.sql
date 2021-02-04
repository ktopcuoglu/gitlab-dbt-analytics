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
  
), final AS (

    SELECT 
      usage_pings_with_license_md5.*, 
      map_license_subscription_account.dim_license_id,
      map_license_subscription_account.dim_subscription_id,
      map_license_subscription_account.is_license_mapped_to_subscription,
      map_license_subscription_account.is_license_subscription_id_valid,
      map_license_subscription_account.dim_crm_account_id,
      map_license_subscription_account.ultimate_parent_account_id,
      IFF(map_license_subscription_account.dim_license_id IS NULL, FALSE, TRUE)   AS is_usage_ping_license_in_licenseDot
    FROM usage_pings_with_license_md5
    LEFT JOIN map_license_subscription_account
      ON usage_pings_with_license_md5.license_md5 = map_license_subscription_account.license_md5
  
)

SELECT * 
FROM final

