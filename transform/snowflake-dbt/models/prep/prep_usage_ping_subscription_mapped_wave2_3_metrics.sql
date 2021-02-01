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

), final AS (

    SELECT
    
    {{ default_usage_ping_information() }}

    -- subscription_info 
    is_usage_ping_license_in_licenseDot,
    dim_license_id,
    license_md5,
    is_license_mapped_to_subscription,
    is_license_subscription_id_valid,
    dim_crm_account_id,
    ultimate_parent_account_id,

    {{ sales_wave_2_3_metrics() }}

    FROM prep_usage_ping

)

SELECT * 
FROM final
