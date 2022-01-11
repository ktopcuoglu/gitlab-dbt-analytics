{{ config(
     materialized = "table"
) }}

WITH usage_ping AS (
  SELECT
    *
  FROM {{ ref('prep_usage_ping') }}
),

license_subscription_mapping AS (
  SELECT
    *
  FROM {{ ref('map_license_subscription_account') }}
),

dates AS (
  SELECT
    *
  FROM {{ ref('dim_date') }}
),

saas_usage_ping AS (
  SELECT
    *
  FROM {{ ref('prep_saas_usage_ping_namespace') }}
),

namespace_subscription_bridge AS (
  SELECT
    *
  FROM {{ ref('bdg_namespace_order_subscription_monthly') }}
),

sm_last_monthly_ping_per_account AS (
  SELECT
    license_subscription_mapping.dim_crm_account_id,
    license_subscription_mapping.dim_subscription_id,
    usage_ping.dim_instance_id AS uuid,
    usage_ping.host_name AS hostname,
    CAST(usage_ping.ping_created_at_month AS DATE) AS snapshot_month,
    usage_ping.raw_usage_data_payload
  FROM usage_ping
  LEFT JOIN license_subscription_mapping ON usage_ping.license_md5 = REPLACE(license_subscription_mapping.license_md5, '-')
  WHERE usage_ping.license_md5 IS NOT NULL
    AND CAST(usage_ping.ping_created_at_month AS DATE) < DATE_TRUNC(month, CURRENT_DATE)
  QUALIFY ROW_NUMBER () OVER (
    PARTITION BY
      license_subscription_mapping.dim_subscription_id,
      usage_ping.dim_instance_id,
      usage_ping.host_name,
      CAST(usage_ping.ping_created_at_month AS DATE)
    ORDER BY
      license_subscription_mapping.dim_subscription_id,
      usage_ping.dim_instance_id,
      usage_ping.host_name,
      usage_ping.ping_created_at DESC
  ) = 1
),

saas_last_monthly_ping_per_account AS (
  SELECT
    namespace_subscription_bridge.dim_crm_account_id,
    namespace_subscription_bridge.dim_subscription_id,
    namespace_subscription_bridge.dim_namespace_id,
    namespace_subscription_bridge.snapshot_month,
    saas_usage_ping.ping_name AS metrics_path,
    saas_usage_ping.counter_value AS metrics_value
  FROM saas_usage_ping
  JOIN dates ON saas_usage_ping.ping_date = dates.date_day
  JOIN namespace_subscription_bridge ON saas_usage_ping.dim_namespace_id = namespace_subscription_bridge.dim_namespace_id
    AND dates.first_day_of_month = namespace_subscription_bridge.snapshot_month
    AND namespace_subscription_bridge.namespace_order_subscription_match_status = 'Paid All Matching'
  WHERE dim_crm_account_id IS NOT NULL
     AND namespace_subscription_bridge.snapshot_month < DATE_TRUNC(month,CURRENT_DATE)
     AND metrics_path LIKE 'usage_activity_by_stage%'
     AND metrics_value > 0 -- - Filter out non-instances
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      namespace_subscription_bridge.dim_subscription_id,
      namespace_subscription_bridge.dim_namespace_id,
      namespace_subscription_bridge.snapshot_month,
      saas_usage_ping.ping_name
    ORDER BY
      namespace_subscription_bridge.dim_subscription_id,
      namespace_subscription_bridge.dim_namespace_id,
      saas_usage_ping.ping_date DESC
  ) = 1
),

unioned AS (
  SELECT
    dim_crm_account_id,
    dim_subscription_id,
    NULL AS dim_namespace_id,
    uuid,
    hostname,
    snapshot_month,
    "PATH" AS metrics_path,
    "VALUE" AS metrics_value
  FROM sm_last_monthly_ping_per_account,
  LATERAL FLATTEN(INPUT => raw_usage_data_payload, RECURSIVE => TRUE)
  WHERE metrics_path LIKE 'usage_activity_by_stage%'
    AND IS_REAL(metrics_value) = 1
    AND metrics_value > 0

  UNION ALL

  SELECT
    dim_crm_account_id,
    dim_subscription_id,
    dim_namespace_id,
    NULL AS uuid,
    NULL AS hostname,
    snapshot_month,
    metrics_path,
    metrics_value
  FROM saas_last_monthly_ping_per_account
)

SELECT
  *
FROM unioned
LIMIT 100
