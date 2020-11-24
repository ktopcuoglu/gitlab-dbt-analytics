/* grain: one record per host per metric per month */

WITH dim_hosts AS (

    SELECT *
    FROM {{ ref('dim_hosts') }}

), dim_instances AS (

    SELECT *
    FROM {{ ref('dim_instances') }}

), dim_licenses AS (

    SELECT *
    FROM {{ ref('dim_licenses') }}

), fct_mrr AS (

    SELECT *
    FROM {{ ref('fct_mrr')}}

), dim_product_details AS (

    SELECT *
    FROM {{ ref('dim_product_details')}}
  
), subscription_source AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), dim_subscriptions AS (

    SELECT *
    FROM {{ ref('dim_subscriptions') }}

), dim_usage_pings AS (

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
    INNER JOIN fct_mrr
      ON dim_subscriptions.subscription_id = fct_mrr.subscription_id
    INNER JOIN dim_product_details
      ON dim_product_details.product_details_id = fct_mrr.product_details_id
    GROUP BY 1,2,3

), joined AS (

    SELECT
      usage_data.*,
      subscription_id,
      account_id,
      array_product_details_id
    FROM usage_data
    LEFT JOIN license_product_details
      ON usage_data.license_md5 = license_product_details.license_md5

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
