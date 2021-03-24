{{config({
    "schema": "legacy"
  })
}}

WITH license AS (

    SELECT *
    FROM {{ ref('license_db_licenses_source') }}

), product_rate_plan_charge AS (

    SELECT *
    FROM {{ ref('zuora_product_rate_plan_charge_source') }}

), rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_source') }}
    WHERE is_deleted = FALSE

), subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), dim_hosts AS (

    SELECT *
    FROM {{ ref('dim_hosts') }}

), usage_data AS (

    SELECT *
    FROM {{ ref('dim_usage_pings') }}

), license_product_details AS (

    SELECT
      license.license_md5,
      subscription.subscription_id,
      subscription.account_id,
      ARRAY_AGG(DISTINCT product_rate_plan_charge_id)            AS array_product_details_id
    FROM license
    INNER JOIN subscription
      ON license.zuora_subscription_id = subscription.subscription_id
    INNER JOIN rate_plan
      ON subscription.subscription_id = rate_plan.subscription_id
    INNER JOIN product_rate_plan_charge
      ON rate_plan.product_rate_plan_id = product_rate_plan_charge.product_rate_plan_id
    GROUP BY 1,2,3

), joined AS (

    SELECT
      usage_data.*,
      subscription_id,
      account_id,
      array_product_details_id,
      dim_hosts.location_id

    FROM usage_data
    LEFT JOIN license_product_details
      ON usage_data.license_md5 = license_product_details.license_md5
    LEFT JOIN dim_hosts
      ON usage_data.host_id = dim_hosts.host_id
        AND usage_data.source_ip_hash = dim_hosts.source_ip_hash
        AND usage_data.uuid = dim_hosts.instance_id

), renamed AS (

    SELECT
      id              AS usage_ping_id,
      created_date_id AS date_id,
      uuid,
      host_id,
      source_ip_hash,
      location_id,
      license_md5,
      subscription_id,
      account_id,
      array_product_details_id,
      hostname,
      main_edition    AS edition,
      product_tier,
      main_edition_product_tier,
      ping_source,
      cleaned_version AS version,
      major_version,
      minor_version,
      major_version || '.' || minor_version AS major_minor_version,
      is_pre_release,
      instance_user_count,
      license_plan,
      license_trial   AS is_trial,
      created_at,
      recorded_at,
      license_user_count,
      gitpod_enabled
    FROM joined

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@derekatwood",
    updated_by="@mpeychet_",
    created_date="2020-08-17",
    updated_date="2021-03-24"
) }}
