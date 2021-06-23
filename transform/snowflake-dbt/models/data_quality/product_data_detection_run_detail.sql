{{
    config({
        "materialized": "incremental",
        "unique_key": "primary_key",
        "schema": "data_quality"
        
    })
}}

WITH rule_run_date AS (

   SELECT distinct
           date_day as rule_run_date,
          'Product' as type_of_data
    FROM {{ ref('dim_date') }}
    WHERE rule_run_date BETWEEN '2021-06-23' AND CURRENT_DATE --date when the code would be pushed to Production,we would be joining this with the dbt updated data for the models.

), dim_host_instance_type AS (
 
    SELECT *
    FROM {{ ref('dim_host_instance_type') }}

), dim_license AS (
 
    SELECT *
    FROM {{ ref('dim_license') }}

), dim_subscription AS (
 
    SELECT *
    FROM {{ ref('dim_subscription') }}

), map_license_subscription_account AS (
 
    SELECT *
    FROM {{ ref('map_license_subscription_account') }}

), bdg_namespace_order_subscription AS (
 
    SELECT *
    FROM {{ ref('bdg_namespace_order_subscription') }}
    WHERE is_subscription_active = 'Y'

), map_subscription_license_all AS (

    SELECT DISTINCT
        dim_subscription.dim_subscription_id, 
        dim_license.dim_license_id 
    FROM dim_subscription 
    LEFT OUTER JOIN dim_license 
    ON dim_subscription.dim_subscription_id = dim_license.dim_subscription_id

), map_license_all AS (

    SELECT DISTINCT
        map_license_subscription_account.dim_license_id, 
        map_license_subscription_account.license_md5, 
        map_license_subscription_account.dim_subscription_id, 
        dim_license.license_start_date,
        dim_license.License_expire_date,
        map_license_subscription_account.dbt_updated_at
    FROM map_license_subscription_account 
    INNER JOIN dim_license 
    ON map_license_subscription_account.dim_license_id = dim_license.dim_license_id
    and map_license_subscription_account.dim_subscription_id = dim_license.dim_subscription_id

), map_subscriptions_all AS (

    SELECT DISTINCT
        dim_subscription.dim_subscription_id, 
        dim_license.dim_license_id, 
        dim_license.license_md5,
        dim_license.license_start_date,
        dim_license.License_expire_date, 
        dim_subscription.subscription_start_date,
        dim_subscription.subscription_end_date,
        dim_license.dbt_updated_at
    FROM dim_license 
    LEFT OUTER JOIN dim_subscription
    ON dim_license.dim_subscription_id  = dim_subscription.dim_subscription_id
    WHERE subscription_end_date IS NOT NULL 
    AND license_expire_date IS NOT NULL


), processed_passed_failed_record_count AS (

--Missing instance types for UUID
    SELECT 
        1                                                                    AS rule_id,
        count(DISTINCT(instance_uuid))                                       AS processed_record_count,
        (SELECT count(DISTINCT(instance_uuid)) FROM dim_host_instance_type
         WHERE INSTANCE_TYPE NOT IN ('Unknown'))                             AS passed_record_count,
        (processed_record_count - passed_record_count)                       AS failed_record_count,
         dbt_updated_at                                                      AS run_date    
    FROM dim_host_instance_type
    GROUP BY run_date

  UNION ALL

--Licenses with missing Subscriptions
    SELECT 
        2                                                                                                AS rule_id,
        count(DISTINCT(dim_license_id))                                                                  AS processed_record_count,
        (SELECT COUNT(DISTINCT(dim_license_id)) FROM dim_license WHERE dim_subscription_id IS NOT NULL)  AS passed_record_count,
        (SELECT COUNT(DISTINCT(dim_license_id)) FROM dim_license WHERE dim_subscription_id IS NULL)      AS failed_record_count,
        dbt_updated_at                                                                                   AS run_date
    FROM dim_license
    GROUP BY run_date

  UNION ALL

--Subscriptions with missing Licenses
    SELECT 
        3                                                                                                                AS rule_id,
        count(DISTINCT(dim_subscription_id))                                                                             AS processed_record_count,
        (SELECT COUNT(DISTINCT(dim_subscription_id)) FROM map_subscription_license_all WHERE dim_license_id IS NOT NULL) AS passed_record_count,
        (SELECT COUNT(DISTINCT(dim_subscription_id)) FROM map_subscription_license_all WHERE dim_license_id is null)     AS failed_record_count,
        dbt_updated_at                                                                                                   AS run_date
    FROM dim_subscription
    GROUP BY run_date
  
  UNION ALL

--Subscriptions with Self-Managed Plans having License Start dates in the future
    SELECT 
        4                                                                                                            AS rule_id,
        count(DISTINCT(dim_subscription_id))                                                                         AS processed_record_count,
        (SELECT COUNT(DISTINCT(dim_subscription_id)) FROM map_license_all WHERE license_start_date <= CURRENT_DATE 
         AND license_start_date IS NOT NULL)                                                                         AS passed_record_count,
        (processed_record_count - passed_record_count)                                                               AS failed_record_count,
        dbt_updated_at                                                                                               AS run_date
    FROM map_license_all 
    WHERE license_start_date IS NOT NULL
    GROUP BY run_date

  UNION ALL

--Subscriptions with Self-Managed Plans having License Start Date greater than License Expire date
    SELECT 
        5                                                                                                                   AS rule_id,
        count(DISTINCT(dim_subscription_id))                                                                                AS processed_record_count,
        (SELECT COUNT(DISTINCT(dim_subscription_id)) FROM map_license_all WHERE license_start_date <= License_expire_date
        AND license_start_date IS NOT NULL AND license_expire_date IS NOT NULL)                                             AS passed_record_count,
        (processed_record_count - passed_record_count)                                                                      AS failed_record_count,
        dbt_updated_at                                                                                                      AS run_date
    FROM map_license_all 
    WHERE license_start_date IS NOT NULL 
    AND license_expire_date IS NOT NULL
    GROUP BY run_date

  UNION ALL

--Expired License IDs with Subscription End Dates in the Past
    SELECT 
        6                                                                                                                    AS rule_id,
        count(DISTINCT(dim_license_id))                                                                                      AS processed_record_count,
        (SELECT COUNT(DISTINCT(dim_license_id)) FROM map_subscription_all WHERE subscription_end_date <= CURRENT_DATE 
        AND License_expire_date <= CURRENT_DATE )                                                                            AS passed_record_count,
        (processed_record_count - passed_record_count)                                                                       AS failed_record_count,
        dbt_updated_at                                                                                                       AS run_date
    FROM map_subscription_all 
    GROUP BY run_date

  UNION ALL

--SaaS Subscriptions Not Mapped to Namespaces
    SELECT 
       7                                                                                                    AS rule_id,
       count(DISTINCT(dim_subscription_id))                                                                 AS processed_record_count,
       (SELECT COUNT(DISTINCT(dim_subscription_id)) FROM bdg_namespace_order_subscription 
       WHERE namespace_order_subscription_match_status = 'Paid All Matching')                               AS passed_record_count,
       (processed_record_count - passed_record_count)                                                       AS failed_record_count,
       dbt_updated_at                                                                                       AS run_date
    FROM bdg_namespace_order_subscription 
    GROUP BY run_date

), final AS (

    SELECT
      --primary_key
      {{ dbt_utils.surrogate_key(['rule_run_date.rule_run_date', 'processed_passed_failed_record_count.rule_id']) }} AS primary_key,

      --Detection Rule record counts
        rule_id,
        processed_record_count,
        passed_record_count,
        failed_record_count,
        rule_run_date.rule_run_date,
        type_of_data
    FROM processed_passed_failed_record_count  
    RIGHT OUTER JOIN rule_run_date
    ON TO_DATE(processed_passed_failed_record_count.run_date) = rule_run_date.rule_run_date

) 

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2021-06-16",
    updated_date="2021-06-16"
) }}
