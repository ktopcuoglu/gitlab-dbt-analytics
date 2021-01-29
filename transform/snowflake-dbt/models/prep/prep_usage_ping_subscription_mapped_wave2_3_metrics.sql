{{ 
    config({
        "materialized": "incremental",
        "unique_key": "dim_usage_ping_id"
    })
}}

WITH prep_usage_ping AS (

    SELECT * 
    FROM {{ ref('prep_usage_ping_subscription_mapped') }}
    WHERE license_md5 IS NOT NULL 

), usage_pings_with_license_md5 AS (

    {{ sales_wave_2_3_metrics() }}

), subscription_info AS (

    SELECT 
        dim_usage_ping_id, 
        is_usage_ping_license_in_licenseDot,
        dim_license_id,
        license_md5,
        is_license_mapped_to_subscription,
        is_license_subscription_id_valid,
        dim_crm_account_id,
        ultimate_parent_account_id
    FROM prep_usage_ping

), final AS (

    SELECT 
        usage_pings_with_license_md5.*,
        is_usage_ping_license_in_licenseDot,
        dim_license_id,
        is_license_mapped_to_subscription,
        is_license_subscription_id_valid,
        dim_crm_account_id,
        ultimate_parent_account_id
    FROM usage_pings_with_license_md5
    INNER JOIN subscription_info
      ON usage_pings_with_license_md5.dim_usage_ping_id = subscription_info.dim_usage_ping_id
  
)

SELECT * 
FROM final
