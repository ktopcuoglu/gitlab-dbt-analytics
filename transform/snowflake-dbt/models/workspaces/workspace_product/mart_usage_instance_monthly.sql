{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_subscription', 'dim_subscription'),
    ('usage_ping_payload', 'prep_usage_ping_payload'),
    ('dim_usage_ping_metric', 'dim_usage_ping_metric')
    ])

}}

, flattened_usage AS (

    SELECT
      dim_usage_ping_id           AS dim_usage_ping_id,
      "path"                      AS metrics_path,
      dim_product_tier_id         AS dim_product_tier_id,
      dim_subscription_id         AS dim_subscription_id,
      dim_location_country_id     AS dim_location_country_id,
      dim_date_id                 AS dim_date_id,
      dim_instance_id             AS dim_instance_id,
      ping_created_at             AS ping_created_at,
      ping_created_at_date        AS ping_created_at_date,
      edition                     AS edition,
      product_tier                AS product_tier,
      major_version               AS major_version,
      minor_version               AS minor_version,
      usage_ping_delivery_type    AS usage_ping_delivery_type,
      is_internal                 AS is_internal,
      is_staging                  AS is_staging,
      is_trial                    AS is_trial,
      instance_user_count         AS instance_user_count,
      host_name                   AS host_name,
      umau_value                  AS umau_value,
      license_subscription_id     AS license_subscription_id,
      key                         AS event_name,
      value                       AS event_count
  FROM usage_ping_payload,
    LATERAL FLATTEN(input => raw_usage_data_payload,
    RECURSIVE => true)
  WHERE SUBSTR(event_count, 1, 1) != '{' AND IS_REAL(TO_VARIANT(event_count)) = true

), flattened_w_metrics AS (

    SELECT
      flattened_usage.*,
      metric.product_stage                                            AS stage_name,
      SUBSTR(metric.product_group, 8, LENGTH(metric.product_group)-7) AS group_name,
      metric.product_section                                          AS section_name,
      is_smau                                                         AS is_smau,
      is_gmau                                                         AS is_gmau,
      is_umau                                                         AS is_umau,
      time_frame                                                      AS time_frame
    FROM flattened_usage
    INNER JOIN dim_usage_ping_metric AS metric
        ON flattened_usage.metrics_path = metric.metrics_path

), flattened_w_subscription AS (
    SELECT
      flattened_w_metrics.*,
      dim_subscription.subscription_name      AS subscription_name,
      dim_subscription.dim_crm_account_id     AS dim_crm_account_id,
      dim_billing_account_id                  AS dim_billing_account_id,
      dim_crm_opportunity_id                  AS dim_crm_opportunity_id,
      dim_subscription_id_original            AS dim_subscription_id_original,
      namespace_id                            AS namespace_id
    FROM flattened_w_metrics
    LEFT JOIN dim_subscription
      ON flattened_w_metrics.dim_subscription_id = dim_subscription.dim_subscription_id

), results AS (

    SELECT
        {{ dbt_utils.surrogate_key(['dim_usage_ping_id','metrics_path']) }}   AS mart_usage_instance_monthly_id,
        event_name                                                            AS event_name,
        TO_NUMBER(event_count)                                                AS event_count,
        dim_usage_ping_id                                                     AS dim_usage_ping_id,
        metrics_path                                                          AS metrics_path,
        dim_product_tier_id                                                   AS dim_product_tier_id,
        dim_subscription_id                                                   AS dim_subscription_id,
        dim_location_country_id                                               AS dim_location_country_id,
        TO_NUMBER(dim_date_id)                                                AS dim_event_date_id,
        dim_instance_id                                                       AS dim_instance_id,
        dim_crm_account_id                                                    AS dim_crm_account_id,
        dim_billing_account_id                                                AS dim_billing_account_id,
        dim_crm_opportunity_id                                                AS dim_crm_opportunity_id,
        dim_subscription_id_original                                          AS dim_subscription_id_original,
        license_subscription_id                                               AS license_subscription_id,
        stage_name                                                            AS stage_name,
        section_name                                                          AS section_name,
        group_name                                                            AS group_name,
        ping_created_at                                                       AS ping_created_at,
        edition                                                               AS edition,
        major_version                                                         AS major_version,
        minor_version                                                         AS minor_version,
        usage_ping_delivery_type                                              AS usage_ping_delivery_type,
        is_smau                                                               AS is_smau,
        is_gmau                                                               AS is_gmau,
        is_umau                                                               AS is_umau,
        'SERVICE PINGS'                                                       AS data_source
    FROM flattened_w_subscription

)

{{ dbt_audit(
    cte_ref="results",
    created_by="@icooper-acp",
    updated_by="@icooper-acp",
    created_date="2022-01-12",
    updated_date="2022-01-18"
) }}
