{{ config(
    tags=["product", "mnpi_exception"],
    full_refresh = false,
    materialized = "incremental",
    unique_key = "mart_service_ping_instance_id"
) }}

{{ simple_cte([
    ('fct_service_ping_instance', 'fct_service_ping_instance'),
    ('dim_service_ping', 'dim_service_ping_instance'),
    ('dim_product_tier', 'dim_product_tier'),
    ('dim_date', 'dim_date'),
    ('dim_billing_account', 'dim_billing_account'),
    ('dim_crm_account', 'dim_crm_account'),
    ('dim_product_detail', 'dim_product_detail'),
    ('fct_charge', 'fct_charge'),
    ('dim_usage_ping_metric', 'dim_usage_ping_metric')
    ])

}}

, dim_subscription AS (

    SELECT *
    FROM {{ ref('dim_subscription') }}
    WHERE (subscription_name_slugify <> zuora_renewal_subscription_name_slugify[0]::TEXT
      OR zuora_renewal_subscription_name_slugify IS NULL)
      AND subscription_status NOT IN ('Draft', 'Expired')


), subscription_source AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), license_subscriptions AS (

    SELECT DISTINCT
      dim_date.date_day                                                            AS reporting_month,
      dim_subscription.subscription_name_slugify,
      dim_subscription.dim_subscription_id                                         AS latest_active_subscription_id,
      dim_subscription.subscription_start_date,
      dim_subscription.subscription_end_date,
      dim_subscription.subscription_start_month,
      dim_subscription.subscription_end_month,
      dim_billing_account.dim_billing_account_id,
      dim_crm_account.crm_account_name,
      dim_crm_account.dim_parent_crm_account_id,
      dim_crm_account.parent_crm_account_name,
      dim_crm_account.parent_crm_account_billing_country,
      dim_crm_account.parent_crm_account_sales_segment,
      dim_crm_account.parent_crm_account_industry,
      dim_crm_account.parent_crm_account_owner_team,
      dim_crm_account.parent_crm_account_sales_territory,
      dim_crm_account.technical_account_manager,
      IFF(MAX(mrr) > 0, TRUE, FALSE)                                                AS is_paid_subscription,
      MAX(IFF(product_rate_plan_name ILIKE ANY ('%edu%', '%oss%'), TRUE, FALSE))    AS is_program_subscription,
      ARRAY_AGG(DISTINCT dim_product_detail.product_tier_name)
        WITHIN GROUP (ORDER BY dim_product_detail.product_tier_name ASC)            AS product_category_array,
      ARRAY_AGG(DISTINCT product_rate_plan_name)
        WITHIN GROUP (ORDER BY product_rate_plan_name ASC)                          AS product_rate_plan_name_array,
      SUM(quantity)                                                                 AS quantity,
      SUM(mrr * 12)                                                                 AS arr
    FROM dim_subscription
    INNER JOIN fct_charge
      ON dim_subscription.dim_subscription_id = fct_charge.dim_subscription_id
        AND charge_type = 'Recurring'
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_charge.dim_product_detail_id
      AND dim_product_detail.product_delivery_type = 'Self-Managed'
      AND product_rate_plan_name NOT IN ('Premium - 1 Year - Eval')
    LEFT JOIN dim_billing_account
      ON dim_subscription.dim_billing_account_id = dim_billing_account.dim_billing_account_id
    LEFT JOIN dim_crm_account
      ON dim_billing_account.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    INNER JOIN dim_date
      ON effective_start_month <= dim_date.date_day AND effective_end_month > dim_date.date_day
      AND dim_date.date_day = dim_date.first_day_of_month
    {{ dbt_utils.group_by(n=17)}}

), fct_service_ping AS  (

  SELECT
    * FROM fct_service_ping_instance
    {% if is_incremental() %}
                WHERE ping_created_at >= COALESCE((SELECT MAX(ping_created_at) FROM {{this}}), '2020-01-01')
    {% endif %}

), fct_pings_w_dims AS  (

SELECT
    fct_service_ping.fct_service_ping_instance_id                             AS fct_service_ping_instance_id,
    fct_service_ping.dim_service_ping_instance_id                             AS dim_service_ping_instance_id,
    fct_service_ping.metrics_path                                             AS metrics_path,
    fct_service_ping.metric_value                                             AS metric_value,
    fct_service_ping.dim_product_tier_id                                      AS dim_product_tier_id,
    fct_service_ping.dim_subscription_id                                      AS dim_subscription_id,
    fct_service_ping.dim_location_country_id                                  AS dim_location_country_id,
    fct_service_ping.dim_date_id                                              AS dim_date_id,
    fct_service_ping.dim_instance_id                                          AS dim_instance_id,
    fct_service_ping.dim_installation_id                                      AS dim_installation_id,
    fct_service_ping.dim_host_id                                              AS dim_host_id,
    fct_service_ping.dim_license_id                                           AS dim_license_id,
    fct_service_ping.ping_created_at                                          AS ping_created_at,
    fct_service_ping.ping_created_at_date                                     AS ping_created_at_date,
    DATE_TRUNC('MONTH', fct_service_ping.ping_created_at::TIMESTAMP)          AS ping_created_at_month,
    dim_service_ping.is_trial                                                 AS is_trial,
    fct_service_ping.umau_value                                               AS umau_value,
    fct_service_ping.dim_subscription_license_id                              AS dim_subscription_license_id,
    fct_service_ping.data_source                                              AS data_source,
    dim_service_ping.edition                                                  AS edition,
    dim_service_ping.host_name                                                AS host_name,
    dim_service_ping.major_version                                            AS major_version,
    dim_service_ping.minor_version                                            AS minor_version,
    dim_service_ping.major_minor_version                                      AS major_minor_version,
    dim_service_ping.major_minor_version_id                                   AS major_minor_version_id,
    dim_service_ping.version_is_prerelease                                    AS version_is_prerelease,
    dim_service_ping.is_internal                                              AS is_internal,
    dim_service_ping.is_staging                                               AS is_staging,
    dim_service_ping.instance_user_count                                      AS instance_user_count,
    dim_service_ping.service_ping_delivery_type                               AS service_ping_delivery_type,
    dim_service_ping.is_last_ping_of_month                                    AS is_last_ping_of_month,
    fct_service_ping.time_frame                                               AS time_frame
FROM fct_service_ping
  INNER JOIN dim_service_ping
ON fct_service_ping.dim_service_ping_instance_id = dim_service_ping.dim_service_ping_instance_id

), fct_w_metric_dims AS (

SELECT
  fct_pings_w_dims.*,
  dim_usage_ping_metric.is_gmau,
  dim_usage_ping_metric.is_smau,
  dim_usage_ping_metric.is_umau,
  dim_usage_ping_metric.product_group,
  dim_usage_ping_metric.product_section,
  dim_usage_ping_metric.product_stage,
  dim_usage_ping_metric.is_paid_gmau
FROM fct_pings_w_dims
  INNER JOIN dim_usage_ping_metric
ON fct_pings_w_dims.metrics_path = dim_usage_ping_metric.metrics_path

), fct_w_product_tier AS (
SELECT
  fct_w_metric_dims.*,
  dim_product_tier.product_tier_name                                          AS product_tier,
  fct_w_metric_dims.edition || ' - ' || dim_product_tier.product_tier_name    AS edition_product_tier
FROM fct_w_metric_dims
  LEFT JOIN dim_product_tier
    ON fct_w_metric_dims.dim_product_tier_id = dim_product_tier.dim_product_tier_id

), joined AS (

    SELECT
      fct_w_product_tier.dim_service_ping_instance_id,
      fct_w_product_tier.dim_date_id,
      fct_w_product_tier.metrics_path,
      fct_w_product_tier.metric_value,
      fct_w_product_tier.product_section AS group_name,
      fct_w_product_tier.product_stage   AS stage_name,
      fct_w_product_tier.product_section AS section_name,
      fct_w_product_tier.is_smau,
      fct_w_product_tier.is_gmau,
      fct_w_product_tier.is_paid_gmau,
      fct_w_product_tier.is_umau,
      fct_w_product_tier.dim_license_id,
      fct_w_product_tier.dim_installation_id,
      fct_w_product_tier.major_minor_version_id,
      fct_w_product_tier.is_trial,
      fct_w_product_tier.umau_value,
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
      COALESCE(is_paid_subscription, FALSE)                         AS is_paid_subscription,
      COALESCE(is_program_subscription, FALSE)                      AS is_program_subscription,
      fct_w_product_tier.edition,
      fct_w_product_tier.product_tier                               AS ping_product_tier,
      fct_w_product_tier.edition_product_tier                       AS ping_main_edition_product_tier,
      fct_w_product_tier.major_version,
      fct_w_product_tier.minor_version,
      fct_w_product_tier.major_minor_version,
      fct_w_product_tier.version_is_prerelease,
      fct_w_product_tier.is_internal,
      fct_w_product_tier.is_staging,
      fct_w_product_tier.instance_user_count,
      fct_w_product_tier.ping_created_at,
      fct_w_product_tier.ping_created_at_month,
      fct_w_product_tier.dim_instance_id,
      fct_w_product_tier.service_ping_delivery_type,
      fct_w_product_tier.host_name,
      fct_w_product_tier.is_last_ping_of_month,
      fct_w_product_tier.time_frame
    FROM fct_w_product_tier
    LEFT JOIN {{ ref('map_usage_ping_active_subscription')}} act_sub
      ON fct_w_product_tier.dim_service_ping_instance_id = act_sub.dim_usage_ping_id
    LEFT JOIN license_subscriptions ON act_sub.dim_subscription_id = license_subscriptions.latest_active_subscription_id
      AND ping_created_at_month = reporting_month

), sorted AS (

    SELECT

      -- Primary Key
      {{ dbt_utils.surrogate_key(['dim_service_ping_instance_id', 'metrics_path']) }} AS mart_service_ping_instance_id,
      dim_date_id,
      metrics_path,
      metric_value,
      dim_service_ping_instance_id,

      --Foreign Key
      dim_instance_id,
      dim_license_id,
      dim_installation_id,
      latest_active_subscription_id,
      dim_billing_account_id,
      dim_parent_crm_account_id,
      major_minor_version_id
      dim_host_id,
      major_minor_version_id,
      host_name,
      -- metadata usage ping
      service_ping_delivery_type,
      edition,
      ping_product_tier,
      ping_main_edition_product_tier,
      major_version,
      minor_version,
      major_minor_version,
      version_is_prerelease,
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
      time_frame,

      --metadata instance
      instance_user_count,

      --metadata subscription
      subscription_name_slugify,
      subscription_start_month,
      subscription_end_month,
      product_category_array,
      product_rate_plan_name_array,
      is_paid_subscription,
      is_program_subscription,

      -- account metadata
      crm_account_name,
      parent_crm_account_name,
      parent_crm_account_billing_country,
      parent_crm_account_sales_segment,
      parent_crm_account_industry,
      parent_crm_account_owner_team,
      parent_crm_account_sales_territory,
      technical_account_manager,

      ping_created_at,
      ping_created_at_month,
      is_last_ping_of_month


    FROM joined
      WHERE time_frame != 'none'

)

{{ dbt_audit(
    cte_ref="sorted",
    created_by="@icooper-acp",
    updated_by="@icooper-acp",
    created_date="2022-03-11",
    updated_date="2022-03-11"
) }}
