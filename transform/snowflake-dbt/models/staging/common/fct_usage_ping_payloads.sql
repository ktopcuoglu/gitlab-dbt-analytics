/* grain: one record per usage pin */

WITH hosts AS (

    SELECT *
    FROM {{ ref('dim_hosts') }}

), instances AS (

    SELECT *
    FROM {{ ref('dim_instances') }}

), licenses AS (

    SELECT *
    FROM {{ ref('dim_licenses') }}

), subscription_source AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), subscriptions AS (

    SELECT *
    FROM {{ ref('dim_subscriptions') }}

), usage_data AS (

    SELECT *
    FROM {{ ref('dim_usage_pings') }}

), license_subscriptions AS (

    SELECT
      licenses.license_md5,
      subscription_source.subscription_id AS original_linked_subscription_id,
      subscription_source.account_id,
      subscription_source.subscription_name_slugify,
      subscriptions.subscription_id       AS latest_active_subscription_id,
      subscription_start_date,
      subscription_end_date,
      subscription_start_month,
      subscription_end_month
    FROM licenses
    INNER JOIN subscription_source
      ON licenses.zuora_subscription_id = subscription_source.subscription_id
    LEFT JOIN subscriptions
      ON subscription_source.subscription_name_slugify = subscriptions.subscription_name_slugify
    GROUP BY 1,2,3

), joined AS (

    SELECT
      usage_data.*,
      subscription_id,
      account_id,
      array_product_details_id
    FROM usage_data

), renamed AS (

    SELECT

      -- Primary Key
      id              AS usage_ping_id,
      created_date_id AS date_id,

      --Foreign Key
      instance_id,
      host_id,
      location_id,
      license_id,
      subscription_id,
      account_id,

      -- metadata usage ping
      delivery,
      main_edition    AS edition,
      product_tier,
      main_edition_product_tier,
      major_version,
      minor_version,
      major_minor_version,
      cleaned_version AS version,
      is_pre_release,

      --metatadata hosts
      source_ip_hash,
      host_name,


      --metadata instance
      instance_user_count,

      --metadata subscription and license
      license_plan,
      license_trial   AS is_trial,
      license_user_count,

      created_at,
      recorded_at
      
    FROM joined

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@derekatwood",
    updated_by="@msendal",
    created_date="2020-08-17",
    updated_date="2020-10-26"
) }}
