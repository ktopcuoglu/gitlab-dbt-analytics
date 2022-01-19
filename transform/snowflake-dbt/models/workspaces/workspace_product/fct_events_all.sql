{{ config(
    tags=["mnpi_exception"],
    "materialized": "table"
) }}


{{ simple_cte([
    ('dim_subscription', 'dim_subscription'),
    ('usage_ping_payload', 'prep_usage_ping_payload'),
    ('xmau_metrics', 'gitlab_dotcom_xmau_metrics'),
    ('usage_data_events', 'gitlab_dotcom_usage_data_events'),
    ('dim_usage_ping_metric', 'dim_usage_ping_metric'),
    ('dim_license', 'dim_license'),
    ('dim_date', 'dim_date'),
    ('namespace_order_subscription', 'bdg_namespace_order_subscription'),
    ('dim_namespace', 'dim_namespace')
    ])

}}

, flattened_usage AS (

    SELECT top 100
      dim_usage_ping_id           AS dim_usage_ping_id
      path                        AS metrics_path,
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
      metric.product_section                                          AS section_name
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

), usage_ping_fact AS (

    SELECT top 100
        {{ dbt_utils.surrogate_key(['dim_usage_ping_id','metrics_path']) }}   AS event_id,
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
        TO_NUMBER(NULL)                                                       AS dim_namespace_id,
        TO_NUMBER(NULL)                                                       AS ultimate_parent_namespace_id,
        license_subscription_id                                               AS license_subscription_id,
        TO_NUMBER(NULL)                                                       AS user_id,
        stage_name                                                            AS stage_name,
        section_name                                                          AS section_name,
        group_name                                                            AS group_name,
        ping_created_at                                                       AS ping_created_at,
        edition,                                                              AS edition
        major_version                                                         AS major_version,
        minor_version                                                         AS minor_version,
        usage_ping_delivery_type                                              AS usage_ping_delivery_type,
        'SERVICE PINGS'                                                       AS source
    FROM flattened_w_subscription

), fct_events AS  (

    SELECT top 100
      event_primary_key                                             AS event_primary_key,
      usage_data_events.event_name                                  AS event_name,
      namespace_id                                                  AS namespace_id,
      user_id                                                       AS user_id,
      parent_type                                                   AS parent_type,
      parent_id                                                     AS parent_id,
      event_created_at                                              AS event_created_at,
      plan_id_at_event_date                                         AS plan_id_at_event_date,
      CASE
          WHEN usage_data_events.stage_name IS NULL
            THEN xmau_metrics.stage_name
          ELSE usage_data_events.stage_name
         end                                                        AS stage_name,
      group_name                                                    AS group_name,
      section_name                                                  AS section_name,
      smau                                                          AS smau,
      gmau                                                          AS gmau,
      is_umau                                                       AS umau
    FROM usage_data_events
    INNER JOIN xmau_metrics
      ON usage_data_events.event_name = xmau_metrics.event_name

), deduped_namespace_bdg AS (

  SELECT
    dim_subscription_id                   AS dim_subscription_id,
    order_id                              AS order_id,
    ultimate_parent_namespace_id          AS ultimate_parent_namespace_id,
    dim_crm_account_id                    AS dim_crm_account_id,
    dim_billing_account_id                AS dim_billing_account_id,
    product_tier_name_subscription        AS product_tier_name_subscription,
    dim_namespace_id                      AS dim_namespace_id,
    is_subscription_active                AS is_subscription_active
    FROM namespace_order_subscription
    WHERE product_tier_name_subscription IN ('SaaS - Bronze', 'SaaS - Ultimate', 'SaaS - Premium')
          AND is_subscription_active = true
          AND is_namespace_active = true
        QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_namespace_id ORDER BY order_id) = 1

), dim_namespace_w_bdg AS (

    SELECT
      dim_namespace.dim_namespace_id                            AS dim_namespace_id,
      dim_namespace.dim_product_tier_id                         AS dim_product_tier_id,
      deduped_namespace_bdg.dim_subscription_id                 AS dim_subscription_id,
      deduped_namespace_bdg.order_id                            AS order_id,
      deduped_namespace_bdg.ultimate_parent_namespace_id        AS ultimate_parent_namespace_id,
      deduped_namespace_bdg.dim_crm_account_id                  AS dim_crm_account_id,
      deduped_namespace_bdg.dim_billing_account_id              AS dim_billing_account_id
    FROM deduped_namespace_bdg
    INNER JOIN dim_namespace
      ON dim_namespace.dim_namespace_id = deduped_namespace_bdg.dim_namespace_id

), final AS (

    SELECT *
    FROM fct_events
    LEFT JOIN dim_namespace_w_bdg
      ON fct_events.namespace_id = dim_namespace_w_bdg.dim_namespace_id

), gitlab_dotcom_fact AS (

    SELECT
      event_primary_key                       AS event_id,
      event_name                              AS event_name,
      1                                       AS event_count,
      TO_NUMBER(NULL)                         AS dim_usage_ping_id,
      NULL                                    AS metrics_path,
      dim_product_tier_id                     AS dim_product_tier_id,
      dim_subscription_id                     AS dim_subscription_id,
      TO_NUMBER(NULL)                         AS dim_location_country_id,
      date_id                                 AS dim_event_date_id,
      NULL                                    AS dim_instance_id,
      dim_crm_account_id                      AS dim_crm_account_id,
      dim_billing_account_id                  AS dim_billing_account_id,
      NULL                                    AS dim_crm_opportunity_id,
      NULL                                    AS dim_subscription_id_original,
      dim_namespace_id                        AS dim_namespace_id,
      ultimate_parent_namespace_id            AS ultimate_parent_namespace_id,
      NULL                                    AS license_subscription_id,
      user_id                                 AS user_id,
      stage_name                              AS stage_name,
      section_name                            AS section_name,
      group_name                              AS group_name,
      event_created_at                        AS ping_created_at,
      NULL                                    AS edition,
      TO_NUMBER(NULL)                         AS major_version,
      TO_NUMBER(NULL)                         AS minor_version,
      NULL                                    AS usage_ping_delivery_type,
      'GITLAB_DOTCOM'                         AS source
    FROM final
    LEFT JOIN dim_date
      ON TO_DATE(event_created_at) = dim_date.date_day

), results AS (

    SELECT *
    FROM gitlab_dotcom_fact

    UNION ALL

    SELECT *
    FROM usage_ping_fact

)

{{ dbt_audit(
    cte_ref="results",
    created_by="@icooper-acp",
    updated_by="@icooper-acp",
    created_date="2022-01-12",
    updated_date="2022-01-18"
) }}
