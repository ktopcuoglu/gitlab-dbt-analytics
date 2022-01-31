{{ config(
    tags=["mnpi_exception"]
) }}

{{config(
        materialized='table'
    )
}}

{{ simple_cte([
    ('dim_subscription', 'dim_subscription'),
    ('usage_ping_payload', 'prep_usage_ping_payload'),
    ('xmau_metrics', 'gitlab_dotcom_xmau_metrics'),
    ('usage_data_events', 'gitlab_dotcom_usage_data_events'),
    ('dim_usage_ping_metric', 'dim_usage_ping_metric'),
    ('dim_license', 'dim_license'),
    ('dim_date', 'dim_date'),
    ('namespace_order_subscription', 'bdg_namespace_order_subscription'),
    ('dim_namespace', 'dim_namespace'),
    ('fct_event_usage_metrics', 'fct_event_usage_metrics')
    ])

}}

, gitlab_dotcom AS (

    SELECT
      event_name,
      SUM(event_count)                                                    AS event_count,
      dim_usage_ping_id,
      metrics_path,
      dim_product_tier_id,
      dim_subscription_id,
      dim_location_country_id,
      dim_event_date_id,
      dim_instance_id,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_crm_opportunity_id,
      dim_subscription_id_original,
      dim_namespace_id,
      ultimate_parent_namespace_id,
      license_subscription_id,
      stage_name,
      section_name,
      group_name,
      edition,
      major_version,
      minor_version,
      usage_ping_delivery_type,
      source
    FROM fct_event_usage_metrics
    GROUP BY 1,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24

), gitlab_dotcom_fact AS (

    SELECT
      NULL                                                                AS dim_instance_id,
      dim_namespace_id                                                    AS dim_namespace_id,
      MD5(
      CAST(
         COALESCE(CAST(dim_namespace_id AS VARCHAR), '') || '-' ||
         COALESCE(CAST(event_name AS VARCHAR), '') || '-' ||
         COALESCE(CAST(dim_event_date_id AS VARCHAR), '') AS VARCHAR))
                                                                          AS event_id,
      event_name                                                          AS event_name,
      TO_NUMBER(event_count)                                              AS event_count,
      dim_event_date_id                                                   AS dim_event_date_id,
      TO_NUMBER(NULL)                                                     AS dim_usage_ping_id,
      NULL                                                                AS metrics_path,
      dim_product_tier_id                                                 AS dim_product_tier_id,
      dim_subscription_id                                                 AS dim_subscription_id,
      TO_NUMBER(NULL)                                                     AS dim_location_country_id,
      dim_crm_account_id                                                  AS dim_crm_account_id,
      dim_billing_account_id                                              AS dim_billing_account_id,
      NULL                                                                AS dim_crm_opportunity_id,
      NULL                                                                AS dim_subscription_id_original,
      ultimate_parent_namespace_id                                        AS ultimate_parent_namespace_id,
      NULL                                                                AS license_subscription_id,
      stage_name                                                          AS stage_name,
      section_name                                                        AS section_name,
      group_name                                                          AS group_name,
      NULL                                                                AS edition,
      TO_NUMBER(NULL)                                                     AS major_version,
      TO_NUMBER(NULL)                                                     AS minor_version,
      NULL                                                                AS usage_ping_delivery_type,
      'GITLAB_DOTCOM'                                                     AS source
    FROM gitlab_dotcom

), flattened_usage AS (

    SELECT
      dim_usage_ping_id                                                   AS dim_usage_ping_id,
      path                                                                AS metrics_path,
      dim_product_tier_id                                                 AS dim_product_tier_id,
      dim_subscription_id                                                 AS dim_subscription_id,
      dim_location_country_id                                             AS dim_location_country_id,
      dim_date_id                                                         AS dim_date_id,
      dim_instance_id                                                     AS dim_instance_id,
      ping_created_at                                                     AS ping_created_at,
      ping_created_at_date                                                AS ping_created_at_date,
      edition                                                             AS edition,
      product_tier                                                        AS product_tier,
      major_version                                                       AS major_version,
      minor_version                                                       AS minor_version,
      usage_ping_delivery_type                                            AS usage_ping_delivery_type,
      is_internal                                                         AS is_internal,
      is_staging                                                          AS is_staging,
      is_trial                                                            AS is_trial,
      instance_user_count                                                 AS instance_user_count,
      host_name                                                           AS host_name,
      umau_value                                                          AS umau_value,
      license_subscription_id                                             AS license_subscription_id,
      key                                                                 AS event_name,
      value                                                               AS event_count
    FROM usage_ping_payload,
      LATERAL FLATTEN(input => raw_usage_data_payload, RECURSIVE => TRUE)
    WHERE SUBSTR(event_count, 1, 1) != '{'
      AND IS_REAL(TO_VARIANT(event_count)) = TRUE

), flattened_w_metrics AS (

    SELECT
      flattened_usage.*,
      metric.product_stage                                                AS stage_name,
      SUBSTR(metric.product_group, 8,
       LENGTH(metric.product_group)-7)                                    AS group_name,
      metric.product_section                                              AS section_name
    FROM flattened_usage
    INNER JOIN PROD.COMMON.DIM_USAGE_PING_METRIC                          AS metric
      ON flattened_usage.metrics_path = metric.metrics_path

), flattened_w_subscription AS (

    SELECT
      flattened_w_metrics.*,
      dim_subscription.subscription_name                                  AS subscription_name,
      dim_subscription.dim_crm_account_id                                 AS dim_crm_account_id,
      dim_billing_account_id                                              AS dim_billing_account_id,
      dim_crm_opportunity_id                                              AS dim_crm_opportunity_id,
      dim_subscription_id_original                                        AS dim_subscription_id_original,
      namespace_id                                                        AS namespace_id
    FROM flattened_w_metrics
    LEFT JOIN dim_subscription
      ON flattened_w_metrics.dim_subscription_id = dim_subscription.dim_subscription_id

), usage_ping_fact AS (

    SELECT
      dim_instance_id                                                     AS dim_instance_id,
      TO_NUMBER(NULL)                                                     AS dim_namespace_id,
      MD5(CAST(COALESCE(CAST(dim_usage_ping_id AS VARCHAR), '') || '-' ||
              COALESCE(CAST(metrics_path AS VARCHAR), '') AS VARCHAR))
                                                                          AS event_id,
      event_name                                                          AS event_name,
      TO_NUMBER(event_count)                                              AS event_count,
      TO_NUMBER(dim_date_id)                                              AS dim_event_date_id,
      dim_usage_ping_id                                                   AS dim_usage_ping_id,
      metrics_path                                                        AS metrics_path,
      dim_product_tier_id                                                 AS dim_product_tier_id,
      dim_subscription_id                                                 AS dim_subscription_id,
      dim_location_country_id                                             AS dim_location_country_id,
      dim_crm_account_id                                                  AS dim_crm_account_id,
      dim_billing_account_id                                              AS dim_billing_account_id,
      dim_crm_opportunity_id                                              AS dim_crm_opportunity_id,
      dim_subscription_id_original                                        AS dim_subscription_id_original,
      TO_NUMBER(NULL)                                                     AS ultimate_parent_namespace_id,
      license_subscription_id                                             AS license_subscription_id,
      stage_name                                                          AS stage_name,
      section_name                                                        AS section_name,
      group_name                                                          AS group_name,
      edition                                                             AS edition,
      major_version                                                       AS major_version,
      minor_version                                                       AS minor_version,
      usage_ping_delivery_type                                            AS usage_ping_delivery_type,
      'SERVICE PINGS'                                                     AS source
    FROM flattened_w_subscription
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25

), results AS (

  SELECT
    *
  FROM gitlab_dotcom_fact

  UNION

  SELECT
    *
  FROM usage_ping_fact

)

{{ dbt_audit(
    cte_ref="results",
    created_by="@dihle",
    updated_by="@dihle",
    created_date="2022-01-28",
    updated_date="2022-01-28"
) }}
