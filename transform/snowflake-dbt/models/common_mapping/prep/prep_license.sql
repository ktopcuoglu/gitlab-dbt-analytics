WITH licenses AS (

    SELECT *
    FROM {{ ref('license_db_licenses_source') }}

), tiers AS (

    SELECT *
    FROM {{ ref('prep_product_tier') }}

), original_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
 
), renamed AS (

    SELECT
      license_id                      AS dim_license_id,
      license_md5,
      zuora_subscription_id           AS dim_subscription_id,
      zuora_subscription_name         AS subscription_name,
      i.original_id                   AS dim_subscription_id_original,
      i.previous_subscription_id      AS dim_subscription_id_previous,
      users_count                     AS license_user_count,
      plan_code                       AS license_plan,
      t.dim_product_tier_id,
      is_trial,
      IFF(
          LOWER(email) LIKE '%@gitlab.com' AND LOWER(company) LIKE '%gitlab%',
          TRUE, FALSE
         )                            AS is_internal,
      company                         AS company,
      starts_at::DATE                 AS license_start_date,
      license_expires_at::DATE        AS license_expire_date,
      created_at,
      updated_at

    FROM licenses l
    LEFT JOIN tiers t 
       ON lower(t.product_tier_name) = (CASE WHEN l.plan_code IS NULL 
                                          OR l.plan_code = '' THEN 'core' 
                                        ELSE l.plan_code END)
    LEFT JOIN original_subscription i 
       ON l.zuora_subscription_id = i.subscription_id 

)


{{ dbt_audit(
    cte_ref="renamed",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2021-01-08",
    updated_date="2021-01-08"
) }}