{{
    config({
        "materialized": "incremental",
        "unique_key": "rule_run_id"
        "schema": "data_quality"
    })
}}

WITH dim_date AS (
 
    SELECT *
    FROM {{ ref('dim_date') }}

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

), rule_run_date AS (

   SELECT distinct
          dim_date.date_day as rule_run_date,
          'Product' as type_of_data
    FROM dim_date

), processed_record_count AS (

  SELECT 
        1 as rule_id,
        count(DISTINCT(instance_uuid))                                        AS processed_record_count,
        (SELECT count(DISTINCT(instance_uuid)) FROM dim_host_instance_type
        WHERE INSTANCE_TYPE in ('Production', 'Non- Production'))             AS passed_record_count,
        (SELECT count(DISTINCT(instance_uuid)) FROM dim_host_instance_type
         WHERE INSTANCE_TYPE in ('Unknown', NULL, ''))                        AS failed_record_count,
         dbt_updated_at
  FROM dim_host_instance_type

  UNION 

  SELECT 
        2                                                                                                AS rule_id,
        count(DISTINCT(dim_license_id)) AS processed_record_count,
        (SELECT COUNT(DISTINCT(dim_license_id)) FROM dim_license WHERE dim_subscription_id IS NOT NULL)  AS passed_record_count,
        (SELECT COUNT(DISTINCT(dim_license_id)) FROM dim_license WHERE dim_subscription_id IS NULL)      AS failed_record_count,
        updated_date
  FROM dim_license

  UNION

 /* SELECT 3 as rule_id,
        count(DISTINCT(dim_subscription_id)) AS processed_record_count,
        (SELECT COUNT(DISTINCT(dim_subscription_id)) FROM dim_subscription WHERE dim_license_id IS NOT NULL)  AS passed_record_count,
        (SELECT COUNT(DISTINCT(dim_susbcription_id)) FROM dim_subscription WHERE dim_license_id IS NULL)      AS failed_record_count
  FROM dim_subscription

  UNION */

  SELECT 
        4 as rule_id,
        count(DISTINCT(dim_subscription_id)) AS processed_record_count,
        (SELECT COUNT(DISTINCT(dim_license_id)) FROM dim_license WHERE license_start_date <= license_expire_date)  AS passed_record_count,
        (SELECT COUNT(DISTINCT(dim_license_id)) FROM dim_license WHERE license_start_date > license_expire_date)   AS failed_record_count,
        dbt_updated_at
  FROM map_license_subscription_account ---join with SM table to get SM plans only -  either with bdg_subscription_product_rate_plan or final SM table

UNION 

  SELECT 
        5 as rule_id,
        count(DISTINCT(dim_license_id)) AS processed_record_count

  FROM dim_license 

UNION 

  SELECT 
        6 as rule_id,
        count(DISTINCT(dim_subscription_id)) AS processed_record_count,
        (SELECT COUNT(DISTINCT(dim_subscription_id) FROM bdg_namespace_order_subscription WHERE dim_subscription_id IS NOT NULL AND dim_namespace_id IS NOT NULL)  AS  passed_record_count,
        (SELECT COUNT(DISTINCT(dim_subscription_id) FROM bdg_namespace_order_subscription WHERE dim_subscription_id IS NOT NULL AND dim_namespace_id IS NULL)      AS  failed_record_count,
  FROM bdg_namespace_order_subscription 

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2021-06-21",
    updated_date="2021-06-21"
) }}