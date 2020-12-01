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
    WHERE subscription_name_slugify <> zuora_renewal_subscription_name_slugify[0]::TEXT
      OR zuora_renewal_subscription_name_slugify IS NULL
      --OR subscription_end_date < DATE_TRUNC('month', current_date)

),  zuora_subscription_snapshots AS (

  /**
  This partition handles duplicates and hard deletes by taking only
    the latest subscription version snapshot
   */

  SELECT
    rank() OVER (
      PARTITION BY subscription_name
      ORDER BY DBT_VALID_FROM DESC) AS rank,
    subscription_id,
    subscription_name
  FROM {{ ref('zuora_subscription_snapshots_source') }}
  WHERE subscription_status NOT IN ('Draft', 'Expired')
    AND CURRENT_TIMESTAMP()::TIMESTAMP_TZ >= dbt_valid_from
    AND {{ coalesce_to_infinity('dbt_valid_to') }} > current_timestamp()::TIMESTAMP_TZ

), fct_invoice_items AS (

    SELECT *
    FROM {{ ref('fct_invoice_items')}}

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
      subscription_source.subscription_id              AS original_linked_subscription_id,
      subscription_source.account_id,
      subscription_source.subscription_name_slugify,
      dim_subscriptions.subscription_id                AS latest_active_subscription_id,
      dim_subscriptions.subscription_start_date,
      dim_subscriptions.subscription_end_date,
      dim_subscriptions.subscription_start_month,
      dim_subscriptions.subscription_end_month,
      dim_billing_accounts.billing_account_id,
      dim_crm_accounts.crm_account_name,
      dim_crm_accounts.ultimate_parent_account_id,
      dim_crm_accounts.ultimate_parent_account_name,
      dim_crm_accounts.ultimate_parent_billing_country,
      dim_crm_accounts.ultimate_parent_account_segment,
      dim_crm_accounts.ultimate_parent_industry,
      dim_crm_accounts.ultimate_parent_account_owner_team,
      dim_crm_accounts.ultimate_parent_territory,
      fct_invoice_items.effective_end_month,
      fct_invoice_items.effective_start_month,
      IFF(MAX(arr) > 0, TRUE, FALSE)                            AS is_paid_subscription,
      MAX(IFF(product_rate_plan_name ILIKE ANY ('%edu%', '%oss%'), TRUE, FALSE))    AS is_edu_oss_subscription,
      ARRAY_AGG(DISTINCT product_category)
        WITHIN GROUP (ORDER BY product_category ASC)            AS product_category_array,
      ARRAY_AGG(DISTINCT product_rate_plan_name)
        WITHIN GROUP (ORDER BY product_rate_plan_name ASC)      AS product_rate_plan_name_array,
      SUM(quantity) AS quantity,
      SUM(arr)      AS arr
    FROM dim_licenses
    INNER JOIN subscription_source
      ON dim_licenses.subscription_id = subscription_source.subscription_id
    INNER JOIN dim_subscriptions
      ON subscription_source.subscription_name_slugify = dim_subscriptions.subscription_name_slugify
    INNER JOIN zuora_subscription_snapshots
      ON zuora_subscription_snapshots.subscription_id = dim_subscriptions.subscription_id
      AND zuora_subscription_snapshots.rank = 1
    LEFT JOIN fct_invoice_items
      ON zuora_subscription_snapshots.subscription_id = fct_invoice_items.dim_subscription_id
    INNER JOIN dim_product_details
      ON dim_product_details.product_details_id = fct_invoice_items.dim_product_details_id
        AND delivery = 'Self-Managed'
        AND product_rate_plan_name NOT IN ('Premium - 1 Year - Eval')
    INNER JOIN dim_billing_accounts
      ON dim_subscriptions.billing_account_id = dim_billing_accounts.billing_account_id
    INNER JOIN dim_crm_accounts
      ON dim_billing_accounts.crm_account_id = dim_crm_accounts.crm_account_id
    {{ dbt_utils.group_by(n=21)}}

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
      license_subscriptions.subscription_name_slugify,
      license_subscriptions.product_category_array,
      license_subscriptions.product_rate_plan_name_array,
      license_subscriptions.subscription_start_month,
      license_subscriptions.subscription_end_month,
      license_subscriptions.billing_account_id,
      license_subscriptions.crm_account_name,
      license_subscriptions.ultimate_parent_account_id,
      license_subscriptions.ultimate_parent_account_name,
      license_subscriptions.ultimate_parent_billing_country,
      license_subscriptions.ultimate_parent_account_segment,
      license_subscriptions.ultimate_parent_industry,
      license_subscriptions.ultimate_parent_account_owner_team,
      license_subscriptions.ultimate_parent_territory,
      COALESCE(is_paid_subscription, FALSE) AS is_paid_subscription,
      is_edu_oss_subscription,
      fct_usage_ping_payloads.delivery,
      fct_usage_ping_payloads.edition,
      fct_usage_ping_payloads.product_tier              AS ping_product_tier,
      fct_usage_ping_payloads.main_edition_product_tier AS ping_main_edition_product_tier,
      fct_usage_ping_payloads.major_version,
      fct_usage_ping_payloads.minor_version,
      fct_usage_ping_payloads.major_minor_version,
      fct_usage_ping_payloads.version,
      fct_usage_ping_payloads.is_pre_release,
      fct_usage_ping_payloads.created_at,
      fct_usage_ping_payloads.recorded_at,
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
        AND created_month >= license_subscriptions.effective_start_month AND created_month < license_subscriptions.effective_end_month

), renamed AS (

    SELECT

      -- Primary Key
      created_month AS reporting_month,
      metrics_path,
      ping_id,

      --Foreign Key
      host_id,
      original_linked_subscription_id,
      latest_active_subscription_id,
      billing_account_id,
      location_id,
      ultimate_parent_account_id,

      -- metadata usage ping
      delivery,
      edition,
      ping_product_tier,
      ping_main_edition_product_tier,
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

      --metadata subscription
      subscription_name_slugify,
      subscription_start_month,
      subscription_end_month,
      product_category_array,
      product_rate_plan_name_array,
      is_paid_subscription,
      is_edu_oss_subscription,
      
      -- account metadata
      crm_account_name,
      ultimate_parent_account_name,
      ultimate_parent_billing_country,
      ultimate_parent_account_segment,
      ultimate_parent_industry,
      ultimate_parent_account_owner_team,
      ultimate_parent_territory,
      
      created_at,
      recorded_at

      -- monthly_usage_data
      monthly_metric_value
      
    FROM joined

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet",
    updated_by="@mpeychet",
    created_date="2020-12-01",
    updated_date="2020-12-01"
) }}
