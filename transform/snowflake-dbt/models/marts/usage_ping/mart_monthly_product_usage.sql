/* grain: one record per host per metric per month */
{{ config({
    "materialized": "incremental",
    "unique_key": "primary_key"
    })
}}

WITH dim_billing_account AS (

    SELECT *
    FROM {{ ref('dim_billing_account') }}

), dim_crm_accounts AS (

    SELECT *
    FROM {{ ref('dim_crm_account') }}

), dim_date AS (

    SELECT DISTINCT first_day_of_month AS date_day
    FROM {{ ref('dim_date') }}

), dim_hosts AS (

    SELECT *
    FROM {{ ref('dim_hosts') }}

), dim_instances AS (

    SELECT *
    FROM {{ ref('dim_instances') }}

), dim_licenses AS (

    SELECT *
    FROM {{ ref('dim_licenses') }}

), dim_location AS (

    SELECT *
    FROM {{ ref('dim_location_country') }}

), dim_product_detail AS (

    SELECT *
    FROM {{ ref('dim_product_detail')}}

), dim_subscription AS (

    SELECT *
    FROM {{ ref('dim_subscription') }}
    WHERE (subscription_name_slugify <> zuora_renewal_subscription_name_slugify[0]::TEXT
      OR zuora_renewal_subscription_name_slugify IS NULL)
      AND subscription_status NOT IN ('Draft', 'Expired')

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

), fct_charge AS (

    SELECT *
    FROM {{ ref('fct_charge')}}

), fct_monthly_usage_data AS (

    SELECT *
    FROM {{ ref('monthly_usage_data') }}
    {% if is_incremental() %}

      WHERE created_month >= (SELECT MAX(reporting_month) FROM {{this}})

    {% endif %}

), dim_usage_pings AS (

    SELECT *
    FROM {{ ref('dim_usage_pings') }}

), subscription_source AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), license_subscriptions AS (

    SELECT DISTINCT
      dim_date.date_day                                                           AS reporting_month,
      license_id,
      dim_licenses.license_md5,
      dim_licenses.company                                                         AS license_company_name,
      subscription_source.subscription_id                                          AS original_linked_subscription_id,
      subscription_source.account_id,
      subscription_source.subscription_name_slugify,
      dim_subscription.dim_subscription_id                                         AS latest_active_subscription_id,
      dim_subscription.subscription_start_date,
      dim_subscription.subscription_end_date,
      dim_subscription.subscription_start_month,
      dim_subscription.subscription_end_month,
      dim_billing_account.dim_billing_account_id,
      dim_crm_accounts.crm_account_name,
      dim_crm_accounts.dim_parent_crm_account_id,
      dim_crm_accounts.parent_crm_account_name,
      dim_crm_accounts.parent_crm_account_billing_country,
      dim_crm_accounts.parent_crm_account_sales_segment,
      dim_crm_accounts.parent_crm_account_industry,
      dim_crm_accounts.parent_crm_account_owner_team,
      dim_crm_accounts.parent_crm_account_sales_territory,
      dim_crm_accounts.technical_account_manager,
      IFF(MAX(mrr) > 0, TRUE, FALSE)                                                AS is_paid_subscription,
      MAX(IFF(product_rate_plan_name ILIKE ANY ('%edu%', '%oss%'), TRUE, FALSE))    AS is_program_subscription,
      ARRAY_AGG(DISTINCT dim_product_detail.product_tier_name)
        WITHIN GROUP (ORDER BY dim_product_detail.product_tier_name ASC)            AS product_category_array,
      ARRAY_AGG(DISTINCT product_rate_plan_name)
        WITHIN GROUP (ORDER BY product_rate_plan_name ASC)                          AS product_rate_plan_name_array,
      SUM(quantity)                                                                 AS quantity,
      SUM(mrr * 12)                                                                 AS arr
    FROM dim_licenses
    INNER JOIN subscription_source
      ON dim_licenses.subscription_id = subscription_source.subscription_id
    LEFT JOIN dim_subscription
      ON subscription_source.subscription_name_slugify = dim_subscription.subscription_name_slugify
    LEFT JOIN subscription_source AS all_subscriptions
      ON subscription_source.subscription_name_slugify = all_subscriptions.subscription_name_slugify
    LEFT JOIN zuora_subscription_snapshots
      ON zuora_subscription_snapshots.subscription_id = dim_subscription.dim_subscription_id
      AND zuora_subscription_snapshots.rank = 1
    INNER JOIN fct_charge
      ON all_subscriptions.subscription_id = fct_charge.dim_subscription_id
        AND charge_type = 'Recurring'
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_charge.dim_product_detail_id
      AND dim_product_detail.product_delivery_type = 'Self-Managed'
      AND product_rate_plan_name NOT IN ('Premium - 1 Year - Eval')
    LEFT JOIN dim_billing_account
      ON dim_subscription.dim_billing_account_id = dim_billing_account.dim_billing_account_id
    LEFT JOIN dim_crm_accounts
      ON dim_billing_account.dim_crm_account_id = dim_crm_accounts.dim_crm_account_id
    INNER JOIN dim_date
      ON effective_start_month <= dim_date.date_day AND effective_end_month > dim_date.date_day
    {{ dbt_utils.group_by(n=22)}}

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
      dim_usage_pings.license_md5,
      dim_usage_pings.license_trial_ends_on,
      dim_usage_pings.is_trial,
      dim_usage_pings.umau_value,
      license_subscriptions.license_id,
      license_subscriptions.license_company_name,
      license_subscriptions.original_linked_subscription_id,
      license_subscriptions.latest_active_subscription_id,
      license_subscriptions.subscription_name_slugify,
      license_subscriptions.product_category_array,
      license_subscriptions.product_rate_plan_name_array,
      license_subscriptions.subscription_start_month,
      license_subscriptions.subscription_end_month,
      license_subscriptions.dim_billing_account_id,
      license_subscriptions.crm_account_name,
      license_subscriptions.dim_parent_crm_account_id,
      license_subscriptions.parent_crm_account_name,
      license_subscriptions.parent_crm_account_billing_country,
      license_subscriptions.parent_crm_account_sales_segment,
      license_subscriptions.parent_crm_account_industry,
      license_subscriptions.parent_crm_account_owner_team,
      license_subscriptions.parent_crm_account_sales_territory,
      license_subscriptions.technical_account_manager,
      COALESCE(is_paid_subscription, FALSE)             AS is_paid_subscription,
      COALESCE(is_program_subscription, FALSE)          AS is_program_subscription,
      dim_usage_pings.ping_source               AS delivery,
      dim_usage_pings.main_edition              AS main_edition,
      dim_usage_pings.edition,
      dim_usage_pings.product_tier              AS ping_product_tier,
      dim_usage_pings.main_edition_product_tier AS ping_main_edition_product_tier,
      dim_usage_pings.major_version,
      dim_usage_pings.minor_version,
      dim_usage_pings.major_minor_version,
      dim_usage_pings.version,
      dim_usage_pings.is_pre_release,
      dim_usage_pings.is_internal,
      dim_usage_pings.is_staging,
      dim_usage_pings.instance_user_count,
      dim_usage_pings.created_at,
      dim_usage_pings.recorded_at,
      time_period,
      monthly_metric_value,
      original_metric_value,
      dim_hosts.host_id,
      dim_hosts.source_ip_hash,
      dim_hosts.instance_id,
      dim_hosts.host_name,
      dim_hosts.location_id,
      dim_location.country_name,
      dim_location.iso_2_country_code
    FROM fct_monthly_usage_data
    LEFT JOIN dim_usage_pings
      ON fct_monthly_usage_data.ping_id = dim_usage_pings.id
    LEFT JOIN dim_hosts
      ON dim_usage_pings.host_id = dim_hosts.host_id
        AND dim_usage_pings.source_ip_hash = dim_hosts.source_ip_hash
        AND dim_usage_pings.uuid = dim_hosts.instance_id
    LEFT JOIN license_subscriptions
      ON dim_usage_pings.license_md5 = license_subscriptions.license_md5
        AND fct_monthly_usage_data.created_month = license_subscriptions.reporting_month
    LEFT JOIN dim_location
      ON dim_hosts.location_id = dim_location.dim_location_country_id

), sorted AS (

    SELECT

      -- Primary Key
      {{ dbt_utils.surrogate_key(['metrics_path', 'created_month', 'instance_id', 'host_id']) }} AS primary_key,
      created_month AS reporting_month,
      metrics_path,
      ping_id,

      --Foreign Key
      host_id,
      instance_id,
      license_id,
      license_md5,
      original_linked_subscription_id,
      latest_active_subscription_id,
      dim_billing_account_id,
      location_id,
      dim_parent_crm_account_id,

      -- metadata usage ping
      delivery,
      main_edition,
      edition,
      ping_product_tier,
      ping_main_edition_product_tier,
      major_version,
      minor_version,
      major_minor_version,
      version,
      is_pre_release,
      is_internal,
      is_staging,
      is_trial,
      umau_value,

      -- metadata metrics

      group_name,
      stage_name,
      section_name,
      is_smau,
      is_gmau,
      is_paid_gmau,
      is_umau,

      --metatadata hosts
      source_ip_hash,
      host_name,


      --metadata instance
      instance_user_count,

      --metadata subscription
      license_company_name,
      subscription_name_slugify,
      subscription_start_month,
      subscription_end_month,
      product_category_array,
      product_rate_plan_name_array,
      is_paid_subscription,
      is_program_subscription,
      license_trial_ends_on,

      -- account metadata
      crm_account_name,
      parent_crm_account_name,
      parent_crm_account_billing_country,
      parent_crm_account_sales_segment,
      parent_crm_account_industry,
      parent_crm_account_owner_team,
      parent_crm_account_sales_territory,
      technical_account_manager,

      -- location info
      country_name            AS ping_country_name,
      iso_2_country_code      AS ping_country_code,

      created_at,
      recorded_at,

      -- monthly_usage_data
      time_period,
      monthly_metric_value,
      original_metric_value

    FROM joined

)

{{ dbt_audit(
    cte_ref="sorted",
    created_by="@mpeychet",
    updated_by="@mcooperDD",
    created_date="2020-12-01",
    updated_date="2020-03-05"
) }}
