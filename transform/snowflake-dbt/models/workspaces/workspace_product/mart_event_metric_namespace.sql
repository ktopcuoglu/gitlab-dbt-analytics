{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_subscription', 'dim_subscription'),
    ('usage_ping_payload', 'prep_usage_ping_payload'),
    ('dim_usage_ping_metric', 'dim_usage_ping_metric'),
    ('fct_event_usage_metrics', 'fct_event_usage_metrics'),
    ('usage_data_events', 'gitlab_dotcom_usage_data_events'),
    ('dim_date', 'dim_date')
    ])
}}

, gitlab_dotcom AS (

    SELECT
      usage_metrics.event_id,
      usage_metrics.event_name,
      usage_metrics.dim_product_tier_id,
      usage_metrics.dim_subscription_id,
      usage_metrics.dim_event_date_id,
      usage_metrics.dim_crm_account_id,
      usage_metrics.dim_billing_account_id,
      usage_metrics.dim_namespace_id,
      usage_metrics.stage_name,
      usage_metrics.section_name,
      usage_metrics.group_name,
      usage_metrics.source,
      usage_data.event_created_at                                                              AS event_created_at,
      usage_data.plan_id_at_event_date                                                         AS plan_id_at_event_date,
      usage_data.plan_name_at_event_date                                                       AS plan_name_at_event_date,
      usage_data.plan_was_paid_at_event_date                                                   AS plan_was_paid_at_event_date,
      usage_data.user_id                                                                       AS user_id,
      SUM(usage_metrics.event_count)                                                           AS event_count
    FROM fct_event_usage_metrics AS usage_metrics
    LEFT JOIN usage_data_events AS usage_data
      ON usage_metrics.event_id = usage_data.event_created_at
    {{ dbt_utils.group_by(n=17) }}

), gitlab_dotcom_fact AS (

    SELECT
      {{ dbt_utils.surrogate_key(['dim_namespace_id', 'event_name', 'dim_event_date_id']) }}  AS usage_id,
      dim_namespace_id                                                                        AS dim_namespace_id,
      event_name                                                                              AS event_name,
      TO_NUMBER(event_count)                                                                  AS event_count,
      dim_event_date_id                                                                       AS dim_event_date_id,
      dim_product_tier_id                                                                     AS dim_product_tier_id,
      dim_subscription_id                                                                     AS dim_subscription_id,
      dim_crm_account_id                                                                      AS dim_crm_account_id,
      dim_billing_account_id                                                                  AS dim_billing_account_id,
      user_id                                                                                 AS user_id,
      stage_name                                                                              AS stage_name,
      section_name                                                                            AS section_name,
      group_name                                                                              AS group_name,
      event_created_at                                                                        AS event_created_at,
      plan_id_at_event_date                                                                   AS plan_id_at_event_date,
      plan_name_at_event_date                                                                 AS plan_name_at_event_date,
      plan_was_paid_at_event_date                                                             AS plan_was_paid_at_event_date,
      'GITLAB_DOTCOM'                                                                         AS event_source
    FROM gitlab_dotcom
    LEFT JOIN dim_date
      ON TO_DATE(event_created_at) = dim_date.date_day
    LIMIT 100

), flattened_usage AS (

    SELECT
      payload.dim_product_tier_id                                                             AS dim_product_tier_id,
      payload.dim_subscription_id                                                             AS dim_subscription_id,
      payload.dim_date_id                                                                     AS dim_date_id,
      payload.ping_created_at                                                                 AS ping_created_at,
      payload.ping_created_at_date                                                            AS ping_created_at_date,
      payload.product_tier                                                                    AS product_tier,
      payload.is_internal                                                                     AS is_internal,
      payload.is_staging                                                                      AS is_staging,
      payload.is_trial                                                                        AS is_trial,
      payload.instance_user_count                                                             AS instance_user_count,
      payload.host_name                                                                       AS host_name,
      payload.umau_value                                                                      AS umau_value,
      flattened.key                                                                           AS event_name,
      flattened.value                                                                         AS event_count
    FROM usage_ping_payload AS payload,
      LATERAL FLATTEN(input => raw_usage_data_payload, RECURSIVE => TRUE) AS flattened
    WHERE SUBSTR(event_count, 1, 1) != '{'
      AND IS_REAL(TO_VARIANT(event_count)) = TRUE

), flattened_w_metrics AS (

    SELECT
      flattened_usage.*,
      metric.product_stage                                                                    AS stage_name,
      SUBSTR(metric.product_group, 8,
       LENGTH(metric.product_group)-7)                                                        AS group_name,
      metric.product_section                                                                  AS section_name
    FROM flattened_usage
    INNER JOIN dim_usage_ping_metric AS metric
      ON flattened_usage.metrics_path = metric.metrics_path

), flattened_w_subscription AS (

    SELECT
      flattened_w_metrics.*,
      dim_subscription.subscription_name                                                      AS subscription_name,
      dim_subscription.dim_crm_account_id                                                     AS dim_crm_account_id,
      dim_billing_account_id                                                                  AS dim_billing_account_id,
      namespace_id                                                                            AS namespace_id
    FROM flattened_w_metrics
    LEFT JOIN dim_subscription
      ON flattened_w_metrics.dim_subscription_id = dim_subscription.dim_subscription_id

), usage_ping_fact AS (

    SELECT
      {{ dbt_utils.surrogate_key(['dim_namespace_id', 'event_name', 'dim_event_date_id']) }}  AS usage_id,
      dim_namespace_id                                                                        AS dim_namespace_id,
      event_name                                                                              AS event_name,
      TO_NUMBER(event_count)                                                                  AS event_count,
      TO_NUMBER(dim_date_id)                                                                  AS dim_event_date_id,
      dim_product_tier_id                                                                     AS dim_product_tier_id,
      dim_subscription_id                                                                     AS dim_subscription_id,
      dim_crm_account_id                                                                      AS dim_crm_account_id,
      dim_billing_account_id                                                                  AS dim_billing_account_id,
      NULL                                                                                    AS dim_user_id,
      stage_name                                                                              AS stage_name,
      section_name                                                                            AS section_name,
      group_name                                                                              AS group_name,
      NULL                                                                                    AS event_created_at,
      NULL                                                                                    AS plan_id_at_event_date,
      NULL                                                                                    AS plan_name_at_event_date,
      NULL                                                                                    AS plan_was_paid_at_event_date,
      'SERVICE PINGS'                                                                         AS event_source
    FROM flattened_w_subscription
    {{ dbt_utils.group_by(n=17) }}

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
    updated_date="2022-02-02"
) }}
