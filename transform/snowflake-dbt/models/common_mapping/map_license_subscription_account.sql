{{ config({
        "materialized": "view",
    })
}}

WITH license AS (

    SELECT
      license_id, 
      license_md5, 
      susbcription_id 
    FROM {{ ref('dim_licenses') }}

), subscription AS (

    SELECT 
      dim_subscription_id, 
      dim_crm_account_id, 
    FROM {{ ref('dim_subscription') }} 

), crm_account AS (

    SELECT 
      crm_account_id, 
      ultimate_parent_account_id 
    FROM {{ ref('dim_crm_account') }} 

), license_mapped_to_subscription AS (

    SELECT 
      license.license_id                        AS dim_license_id, 
      license.license_md5                       AS license_md5, 
      subscription.dim_subscription_id          AS dim_subscription_id, 
      subcription.dim_crm_account_id            AS dim_crm_account_id
    FROM license 
    INNER JOIN subscription
      ON license.susbcription_id = subscription.dim_subscription_id

), subscription_mapped_to_crm_account AS (

    SELECT 
      subscription.dim_subscription_id          AS dim_subscription_id, 
      subcription.dim_crm_account_id            AS dim_crm_account_id, 
      crm_account.ultimate_parent_account_id    AS ultimate_parent_account_id
    FROM subscription 
    INNER JOIN crm_account
      ON subscription.dim_crm_account_id = crm_account.crm_account_id
    
), joined AS (

    SELECT 
      license_mapped_to_subscription.dim_license_id, 
      license_mapped_to_subscription.license_md5, 
      license_mapped_to_subscription.dim_subscription_id, 
      license_mapped_to_subscription.dim_crm_account_id, 
      subscription_mapped_to_crm_account.ultimate_parent_account_id
    FROM license_mapped_to_subscription
    LEFT JOIN subscription_mapped_to_crm_account
        ON license_mapped_to_subscription.dim_subscription_id = subscription_mapped_to_crm_account.dim_subscription_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@kathleentam",
    updated_by="@kathleentam",
    created_date="2021-01-10",
    updated_date="2021-01-10"
) }}
