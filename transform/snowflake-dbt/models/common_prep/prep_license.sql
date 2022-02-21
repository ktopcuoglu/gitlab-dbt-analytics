WITH customers_db_licenses AS (

    SELECT *
    FROM {{ ref('customers_db_licenses_source') }}

), original_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}

), licenses AS (

    SELECT
      license_id                                                       AS dim_license_id,
      license_md5,
      zuora_subscription_id                                            AS dim_subscription_id,
      zuora_subscription_name                                          AS subscription_name,
      'Customers Portal'                                               AS environment,
      license_user_count,
      IFF(plan_code IS NULL OR plan_code = '', 'core', plan_code)      AS license_plan,
      is_trial,
      IFF(
          LOWER(email) LIKE '%@gitlab.com' AND LOWER(company) LIKE '%gitlab%',
          TRUE, FALSE
         )                                                             AS is_internal,
      company,
      license_start_date,
      license_expire_date,
      created_at,
      updated_at
    FROM customers_db_licenses
    
), renamed AS (

    SELECT
      -- Primary Key
      licenses.dim_license_id,

      -- Foreign Keys
      licenses.dim_subscription_id,
      original_subscription.original_id                                AS dim_subscription_id_original,
      original_subscription.previous_subscription_id                   AS dim_subscription_id_previous,

      -- Descriptive information
      licenses.license_md5,
      licenses.subscription_name,
      licenses.environment,
      licenses.license_user_count,
      licenses.license_plan,
      licenses.is_trial,
      licenses.is_internal,
      licenses.company,
      licenses.license_start_date,
      licenses.license_expire_date,
      licenses.created_at,
      licenses.updated_at

    FROM licenses
    LEFT JOIN original_subscription
       ON licenses.dim_subscription_id = original_subscription.subscription_id 

)


{{ dbt_audit(
    cte_ref="renamed",
    created_by="@snalamaru",
    updated_by="@chrissharp",
    created_date="2021-01-08",
    updated_date="2022-01-20"
) }}
