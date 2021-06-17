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
    WHERE rule_run_date between '2021-04-01' and CURRENT_DATE

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
        dim_license.License_expire_date
    FROM map_license_subscription_account 
    LEFT OUTER JOIN dim_license
    ON map_license_subscription_account.dim_license_id = dim_license.dim_license_id

), map_subscription_all AS (

    SELECT DISTINCT
        map_license_all.dim_subscription_id, 
        map_license_all.dim_license_id, 
        map_license_all.license_md5,
        map_license_all.license_start_date,
        map_license_all.License_expire_date, 
        dim_subscription.subscription_start_date,
        dim_subscription.subscription_end_date
    FROM map_license_all
    LEFT OUTER JOIN dim_subscription
    ON map_license_all.dim_subscription_id = dim_subscription.dim_subscription_id 

), processed_passed_failed_record_count AS (

    SELECT 
        1                                                                     AS rule_id,
        count(DISTINCT(instance_uuid))                                        AS processed_record_count,
        (SELECT count(DISTINCT(instance_uuid)) FROM dim_host_instance_type
        WHERE INSTANCE_TYPE in ('Production', 'Non- Production'))             AS passed_record_count,
        (SELECT count(DISTINCT(instance_uuid)) FROM dim_host_instance_type
         WHERE INSTANCE_TYPE in ('Unknown', NULL, ''))                        AS failed_record_count,
         dbt_updated_at as run_date
    FROM dim_host_instance_type
    GROUP BY run_date

  UNION 

    SELECT 
        2                                                                                                AS rule_id,
        count(DISTINCT(dim_license_id)) AS processed_record_count,
        (SELECT COUNT(DISTINCT(dim_license_id)) FROM dim_license WHERE dim_subscription_id IS NOT NULL)  AS passed_record_count,
        (SELECT COUNT(DISTINCT(dim_license_id)) FROM dim_license WHERE dim_subscription_id IS NULL)      AS failed_record_count,
        dbt_updated_at                                                                                   AS run_date
    FROM dim_license
    GROUP BY run_date

  UNION

    SELECT 
        3                                                                                                                AS rule_id,
        count(DISTINCT(dim_subscription_id))                                                                             AS processed_record_count,
        (SELECT COUNT(DISTINCT(dim_subscription_id)) FROM map_subscription_license_all WHERE dim_license_id IS NOT NULL) AS passed_record_count,
        (SELECT COUNT(DISTINCT(dim_subscription_id)) FROM map_subscription_license_all WHERE dim_license_id is null)     AS failed_record_count,
        dbt_updated_at                                                                                                   AS run_date
    FROM dim_subscription
    GROUP BY run_date
  
  UNION 

    SELECT 
        4                                                                                                       AS rule_id,
        count(DISTINCT(dim_subscription_id)) AS processed_record_count,
        (SELECT COUNT(DISTINCT(dim_license_id)) FROM map_license_all WHERE license_start_date <= CURRENT_DATE)  AS passed_record_count,
        (SELECT COUNT(DISTINCT(dim_license_id)) FROM map_license_all WHERE license_start_date > CURRENT_DATE)   AS failed_record_count,
        dbt_updated_at                                                                                          AS run_date
    FROM map_license_subscription_account 
    GROUP BY run_date

  UNION 

    SELECT 
        5                                                                                                              AS rule_id,
        count(DISTINCT(dim_subscription_id))                                                                           AS processed_record_count,
        (SELECT COUNT(DISTINCT(dim_license_id)) FROM map_license_all WHERE license_start_date <= License_expire_date)  AS passed_record_count,
        (SELECT COUNT(DISTINCT(dim_license_id)) FROM map_license_all WHERE license_start_date > License_expire_date)   AS failed_record_count,
        dbt_updated_at                                                                                                 AS run_date
    FROM map_license_subscription_account 
    GROUP BY run_date

  UNION 

    SELECT 
        6                                                                                                                                                           AS rule_id,
        count(DISTINCT(dim_license_id))                                                                                                                             AS processed_record_count,
        (SELECT COUNT(DISTINCT(dim_subscription_id)) FROM map_subscription_all WHERE subscription_end_date >= CURRENT_DATE AND License_expire_date <= CURRENT_DATE) AS passed_record_count,
        (SELECT COUNT(DISTINCT(dim_subscription_id)) FROM map_subscription_all WHERE subscription_end_date <= CURRENT_DATE AND License_expire_date <= CURRENT_DATE) AS failed_record_count,
        dbt_updated_at                                                                                                                                              AS run_date
    FROM map_license_subscription_account 
    GROUP BY run_date

  UNION 

    SELECT 
       7                                                                                                                                                           AS rule_id,
       count(DISTINCT(dim_subscription_id))                                                                                                                        AS processed_record_count,
       (SELECT COUNT(DISTINCT(dim_subscription_id)) FROM bdg_namespace_order_subscription WHERE dim_subscription_id IS NOT NULL AND dim_namespace_id IS NOT NULL)  AS passed_record_count,
       (SELECT COUNT(DISTINCT(dim_subscription_id)) FROM bdg_namespace_order_subscription WHERE dim_subscription_id IS NOT NULL AND dim_namespace_id IS NULL)      AS failed_record_count,
       dbt_updated_at                                                                                                                                              AS run_date
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
        rule_run_date,
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