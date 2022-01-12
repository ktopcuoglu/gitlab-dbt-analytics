/*-------------------------------- */
-- service pings (usage ping usage)
/*-------------------------------- */

WITH dim_subscriptiON AS (
    SELECT dim_subscription_id,
      subscription_name,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_crm_opportunity_id,
      dim_subscription_id_original,
      namespace_id AS dim_namespace_id,
      namespace_name
  FROM prod.common.dim_subscription
),
flattened_usage AS (
    SELECT top 100
        dim_usage_ping_id,
        path AS metrics_path,
        dim_product_tier_id,
        dim_subscription_id,
        dim_location_country_id,
        dim_date_id,
        dim_instance_id,
        ping_created_at,
        ping_created_at_date,
        edition,
        product_tier,
        major_version,
        minor_version,
        usage_ping_delivery_type,
        is_internal,
        is_staging,
        is_trial,
        instance_user_count,
        host_name,
        umau_value,
        license_subscription_id,
        key AS event_name,
        value AS event_count
  FROM prod.common_prep.prep_usage_ping_payload,
    lateral flatten(input => raw_usage_data_payload,
    recursive => true)
  WHERE substr(value, 1, 1) != '{' and is_real(to_variant(value)) = true
),
flattened_w_metrics AS (
    SELECT flattened_usage.*,
            metric.product_stage AS stage_name,
            substr(metric.product_group, 8, length(metric.product_group)-7) AS group_name,
            metric.product_sectiON AS section_name
    FROM flattened_usage
        JOIN
    prod.common.dim_usage_ping_metric AS metric
        ON flattened_usage.metrics_path = metric.metrics_path

),
flattened_w_subscriptiON AS (
    SELECT flattened_w_metrics.*,
      dim_subscription.subscription_name,
      dim_subscription.dim_crm_account_id,
      dim_billing_account_id,
      dim_crm_opportunity_id,
      dim_subscription_id_original,
      dim_namespace_id,
      namespace_name
  FROM flattened_w_metrics
    JOIN
  dim_subscription
    ON flattened_w_metrics.dim_subscription_id = dim_subscription.dim_subscription_id
),
usage_ping_fact AS (
    SELECT
        to_varchar(dim_usage_ping_id) AS event_id,
        event_name,
        to_number(event_count) AS event_count,
        dim_usage_ping_id,
        metrics_path,
        dim_product_tier_id,
        dim_subscription_id,
        dim_location_country_id,
        to_number(dim_date_id) AS dim_date_id,
        dim_instance_id,
        dim_crm_account_id,
        dim_billing_account_id,
        dim_crm_opportunity_id,
        dim_subscription_id_original,
        to_number(null) AS dim_namespace_id,
        to_number(null) AS ultimate_parent_namespace_id,
        license_subscription_id,
        to_number(null)  AS user_id,
        namespace_name,
        stage_name,
        section_name,
        group_name,
        ping_created_at,
        edition,
        major_version,
        minor_version,
        usage_ping_delivery_type,
        'SERVICE PINGS' AS source
    FROM flattened_w_subscription
),

/*-------------------------------- */
-- gitlab.com source
/*-------------------------------- */

fct_events AS  (
    SELECT top 100 event_primary_key,
            prod.legacy.gitlab_dotcom_usage_data_events.event_name AS event_name,
            namespace_id,
            user_id,
            parent_type,
            parent_id,
            event_created_at,
            plan_id_at_event_date,
            case
                when prod.legacy.gitlab_dotcom_usage_data_events.stage_name is null then prod.legacy.gitlab_dotcom_xmau_metrics.stage_name
                else prod.legacy.gitlab_dotcom_usage_data_events.stage_name
               end AS stage_name,
            group_name,
            section_name,
            smau,
            gmau,
            is_umau AS umau
        FROM prod.legacy.gitlab_dotcom_usage_data_events
            JOIN prod.legacy.gitlab_dotcom_xmau_metrics
        ON prod.legacy.gitlab_dotcom_usage_data_events.event_name = prod.legacy.gitlab_dotcom_xmau_metrics.event_name
    ),
dim_license AS (
    SELECT dim_license_id,
           dim_subscription_id,
           dim_subscription_id_original,
           dim_subscription_id_previous,
           dim_product_tier_id,
           dim_environment_id
    FROM prod.common.dim_license
    -- JOIN this to subscriptions via id
),
xmau_metrics AS (
SELECT *
    FROM prod.legacy.gitlab_dotcom_xmau_metrics
),
dim_namespace_w_bdg AS (
    SELECT dim_namespace.dim_namespace_id,
            dim_namespace.dim_product_tier_id,
            bdg_namespace_order_subscription.dim_subscription_id,
            bdg_namespace_order_subscription.order_id,
            bdg_namespace_order_subscription.ultimate_parent_namespace_id,
            bdg_namespace_order_subscription.dim_crm_account_id,
            bdg_namespace_order_subscription.dim_billing_account_id
    FROM prod.common.bdg_namespace_order_subscription
        INNER JOIN prod.common.dim_namespace
    ON dim_namespace.dim_namespace_id = bdg_namespace_order_subscription.dim_namespace_id
),
dim_all AS (
    SELECT dim_namespace_w_bdg.dim_namespace_id,
            dim_namespace_w_bdg.dim_product_tier_id,
            dim_namespace_w_bdg.dim_subscription_id,
            dim_namespace_w_bdg.order_id,
            dim_namespace_w_bdg.ultimate_parent_namespace_id,
            dim_namespace_w_bdg.dim_crm_account_id,
            dim_namespace_w_bdg.dim_billing_account_id,
           dim_license.dim_license_id,
           dim_license.dim_subscription_id_original,
           dim_license.dim_subscription_id_previous,
           dim_license.dim_product_tier_id,
           dim_license.dim_environment_id
    FROM dim_namespace_w_bdg
        INNER JOIN dim_license
    ON dim_namespace_w_bdg.dim_subscription_id = dim_license.dim_subscription_id
),
final AS (
SELECT * FROM fct_events
    JOIN dim_namespace_w_bdg
        ON fct_events.namespace_id = dim_namespace_w_bdg.dim_namespace_id
),
gitlab_dotcom_fact AS (
  SELECT
        event_primary_key AS event_id,
        event_name,
        1 AS event_count,
        to_number(null) AS dim_usage_ping_id,
        null AS metrics_path, --- pretty sure not relevant
        dim_product_tier_id,
        dim_subscription_id,
        to_number(null) AS dim_location_country_id,
        concat(substr(to_varchar(event_created_at), 1, 4),substr(to_varchar(event_created_at), 6, 2), substr(to_varchar(event_created_at), 9, 2)) AS dim_date_id,
        null AS dim_instance_id,
        dim_crm_account_id,
        dim_billing_account_id,
        null AS dim_crm_opportunity_id,
        null AS dim_subscription_id_original,
        dim_namespace_id,
        ultimate_parent_namespace_id,
        null AS license_subscription_id,
        user_id,
        null AS namespace_name,
        stage_name,
        section_name,
        group_name,
        event_created_at AS ping_created_at,
        null AS edition,
        to_number(null) AS major_version,
        to_number(null) AS minor_version,
        null AS usage_ping_delivery_type,
        'GITLAB_DOTCOM' AS source
FROM final)

SELECT *
FROM gitlab_dotcom_fact

UNION ALL

SELECT *
FROM usage_ping_fact
