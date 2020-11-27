/* grain: one record per host per metric per month */

WITH dim_billing_accounts AS (

  SELECT *
  FROM {{ ref('dim_billing_accounts') }}

), dim_crm_accounts AS (

  SELECT *
  FROM {{ ref('dim_crm_accounts') }}

), dim_hosts AS (

    SELECT *
    FROM {{ ref('dim_hosts') }}

), dim_instances AS (

    SELECT *
    FROM {{ ref('dim_instances') }}

), dim_licenses AS (

    SELECT *
    FROM {{ ref('dim_licenses') }}

), dim_product_details AS (

    SELECT *
    FROM {{ ref('dim_product_details')}}
  
), dim_subscriptions AS (

    SELECT *
    FROM {{ ref('dim_subscriptions') }}

), fct_mrr AS (

    SELECT *
    FROM {{ ref('fct_mrr')}}

), fct_monthly_usage_data AS (

    SELECT *
    FROM {{ ref('monthly_usage_data') }}
  
), fct_usage_ping_payloads AS (

    SELECT *
    FROM {{ ref('dim_usage_pings') }}

), subscription_source AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), license_subscriptions AS (

    SELECT DISTINCT
      license_id,
      dim_licenses.license_md5,
      subscription_source.subscription_id AS original_linked_subscription_id,
      subscription_source.account_id,
      subscription_source.subscription_name_slugify,
      dim_subscriptions.subscription_id       AS latest_active_subscription_id,
      dim_subscriptions.subscription_start_date,
      dim_subscriptions.subscription_end_date,
      dim_subscriptions.subscription_start_month,
      dim_subscriptions.subscription_end_month,
      dim_billing_accounts.billing_account_id
    FROM dim_licenses
    INNER JOIN subscription_source
      ON dim_licenses.subscription_id = subscription_source.subscription_id
    LEFT JOIN dim_subscriptions
      ON subscription_source.subscription_name_slugify = dim_subscriptions.subscription_name_slugify
    INNER JOIN fct_mrr
      ON dim_subscriptions.subscription_id = fct_mrr.subscription_id
    INNER JOIN dim_product_details
      ON dim_product_details.product_details_id = fct_mrr.product_details_id
    INNER JOIN dim_billing_accounts
      ON dim_subscriptions.billing_account_id = dim_billing_accounts.billing_account_id
    INNER JOIN dim_crm_accounts
      ON dim_billing_accounts.crm_account_id = dim_crm_accounts.crm_account_id

), joined AS (

    SELECT
      fct_monthly_usage_data.ping_id,
      fct_monthly_usage_data.created_month,
      fct_monthly_usage_data.metrics_path,
      fct_monthly_usage_data.group_name,
      fct_monthly_usage_data.stage_name,
      fct_monthly_usage_data.section_name,
      fct_monthly_usage_data.is_smau,
      fct_monthly_usage_data.is_gmau,
      fct_monthly_usage_data.is_paid_gmau,
      fct_monthly_usage_data.is_umau,
      license_subscriptions.original_linked_subscription_id,
      license_subscriptions.latest_active_subscription_id,
      license_subscriptions.billing_account_id,
      fct_usage_ping_payloads.delivery,
      fct_usage_ping_payloads.edition,
      fct_usage_ping_payloads.product_tier,
      fct_usage_ping_payloads.main_edition_product_tier,
      fct_usage_ping_payloads.major_version,
      fct_usage_ping_payloads.minor_version,
      fct_usage_ping_payloads.major_minor_version,
      fct_usage_ping_payloads.version,
      fct_usage_ping_payloads.is_pre_release,
      monthly_metric_value,
      dim_hosts.host_id,
      dim_hosts.host_name,
      dim_hosts.location_id
    FROM fct_monthly_usage_data
    LEFT JOIN fct_usage_ping_payloads
      ON fct_monthly_usage_data.ping_id = fct_usage_ping_payloads.id
    LEFT JOIN dim_hosts
      ON fct_usage_ping_payloads.host_id = dim_hosts.host_id
        AND fct_usage_ping_payloads.source_ip_hash = dim_hosts.source_ip_hash
        AND fct_usage_ping_payloads.uuid = dim_hosts.instance_id
    LEFT JOIN license_subscriptions
      ON fct_usage_ping_payloads.license_md5 = license_subscriptions.license_md5      

), renamed AS (

    SELECT

      -- Primary Key
      created_month AS reporting_month,
      metrics_path,

      --Foreign Key
      host_id,
      original_linked_subscription_id,
      billing_account_id,
      location_id,

      -- metadata usage ping
      --delivery,
      edition,
      product_tier,
      main_edition_product_tier,
      major_version,
      minor_version,
      major_minor_version,
      version,
      is_pre_release

      --metatadata hosts
      source_ip_hash,
      host_name,


      --metadata instance
      --instance_user_count,

      --metadata subscription and license
      --license_plan,
      --license_trial   AS is_trial,
      --license_user_count,

      --created_at,
      --recorded_at

      -- monthly_usage_data
      monthly_metric_value
      
    FROM joined

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@derekatwood",
    updated_by="@msendal",
    created_date="2020-08-17",
    updated_date="2020-10-26"
) }}
