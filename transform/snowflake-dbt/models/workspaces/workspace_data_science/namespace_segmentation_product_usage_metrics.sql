{{ config(
     materialized = "table",
     tags=["mnpi_exception"]
) }}

WITH days_of_usage AS (
  
    SELECT
    
      namespace_id,
      COUNT(DISTINCT(CASE WHEN stage_name = 'create' THEN days_since_namespace_creation ELSE NULL END))    AS days_usage_in_stage_create_all_time_cnt,
      COUNT(DISTINCT(CASE WHEN stage_name = 'protect' THEN days_since_namespace_creation ELSE NULL END))   AS days_usage_in_stage_protect_all_time_cnt,
      COUNT(DISTINCT(CASE WHEN stage_name = 'package' THEN days_since_namespace_creation ELSE NULL END))   AS days_usage_in_stage_package_all_time_cnt,
      COUNT(DISTINCT(CASE WHEN stage_name = 'plan' THEN days_since_namespace_creation ELSE NULL END))      AS days_usage_in_stage_plan_all_time_cnt,
      COUNT(DISTINCT(CASE WHEN stage_name = 'secure' THEN days_since_namespace_creation ELSE NULL END))    AS days_usage_in_stage_secure_all_time_cnt,
      COUNT(DISTINCT(CASE WHEN stage_name = 'verify' THEN days_since_namespace_creation ELSE NULL END))    AS days_usage_in_stage_verify_all_time_cnt,
      COUNT(DISTINCT(CASE WHEN stage_name = 'configure' THEN days_since_namespace_creation ELSE NULL END)) AS days_usage_in_stage_configure_all_time_cnt,
      COUNT(DISTINCT(CASE WHEN stage_name = 'release' THEN days_since_namespace_creation ELSE NULL END))   AS days_usage_in_stage_release_all_time_cnt
    FROM {{ ref('gitlab_dotcom_usage_data_events') }}
    WHERE stage_name NOT IN ('monitor', 'manage')
      AND project_is_learn_gitlab != TRUE 
    GROUP BY 1

)

SELECT *
FROM days_of_usage
