WITH customers_db_licenses AS (

    SELECT *
    FROM {{ ref('customers_db_licenses_source') }}

), licenses AS (

    SELECT *
    FROM {{ ref('license_db_licenses_source') }}

), original_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
 
), licenses_without_customers_db AS (

    SELECT *
    FROM licenses
    WHERE license_id NOT IN (SELECT license_id FROM customers_db_licenses)

), union_licenses AS (

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
    
    UNION

    SELECT
      license_id                                                       AS dim_license_id,
      license_md5,
      zuora_subscription_id                                            AS dim_subscription_id,
      zuora_subscription_name                                          AS subscription_name,
      'License DB'                                                     AS environment,
      users_count                                                      AS license_user_count,
      IFF(plan_code IS NULL OR plan_code = '', 'core', plan_code)      AS license_plan,
      is_trial,
      IFF(
          LOWER(email) LIKE '%@gitlab.com' AND LOWER(company) LIKE '%gitlab%',
          TRUE, FALSE
         )                                                             AS is_internal,
      company                                                          AS company,
      starts_at::DATE                                                  AS license_start_date,
      license_expires_at::DATE                                         AS license_expire_date,
      created_at,
      updated_at

    FROM licenses_without_customers_db
    
), renamed AS (

    SELECT
      -- Primary Key
      union_licenses.dim_license_id,

      -- Foreign Keys
      union_licenses.dim_subscription_id,
      original_subscription.original_id                                AS dim_subscription_id_original,
      original_subscription.previous_subscription_id                   AS dim_subscription_id_previous,

      -- Descriptive information
      union_licenses.license_md5,
      union_licenses.subscription_name,
      union_licenses.environment,
      union_licenses.license_user_count,
      union_licenses.license_plan,
      union_licenses.is_trial,
      union_licenses.is_internal,
      union_licenses.company,
      union_licenses.license_start_date,
      union_licenses.license_expire_date,
      union_licenses.created_at,
      union_licenses.updated_at

    FROM union_licenses
    LEFT JOIN original_subscription
       ON union_licenses.dim_subscription_id = original_subscription.subscription_id 

)


{{ dbt_audit(
    cte_ref="renamed",
    created_by="@snalamaru",
    updated_by="@jpeguero",
    created_date="2021-01-08",
    updated_date="2021-09-22"
) }}
