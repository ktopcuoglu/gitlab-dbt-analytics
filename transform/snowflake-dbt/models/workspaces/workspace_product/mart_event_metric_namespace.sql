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
    ('dim_usage_ping_metric', 'dim_usage_ping_metric'),
    ('fct_event_usage_metrics', 'fct_event_usage_metrics')
    ])
}}

, gitlab_dotcom AS (

    SELECT
      event_name,

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
      source,
      SUM(event_count)                                                                        AS event_count
    FROM fct_event_usage_metrics
    {{ dbt_utils.group_by(n=23) }}
), gitlab_dotcom_fact AS (

    SELECT
      NULL                                                                                    AS dim_instance_id,
      dim_namespace_id                                                                        AS dim_namespace_id,
      {{ dbt_utils.surrogate_key(['dim_namespace_id', 'event_name', 'dim_event_date_id']) }}  AS event_id,
      event_name                                                                              AS event_name,
      TO_NUMBER(event_count)                                                                  AS event_count,
      dim_event_date_id                                                                       AS dim_event_date_id,
      TO_NUMBER(NULL)                                                                         AS dim_usage_ping_id,
      NULL                                                                                    AS metrics_path,
      dim_product_tier_id                                                                     AS dim_product_tier_id,
      dim_subscription_id                                                                     AS dim_subscription_id,
      TO_NUMBER(NULL)                                                                         AS dim_location_country_id,
      dim_crm_account_id                                                                      AS dim_crm_account_id,
      dim_billing_account_id                                                                  AS dim_billing_account_id,
      NULL                                                                                    AS dim_crm_opportunity_id,
      NULL                                                                                    AS dim_subscription_id_original,
      ultimate_parent_namespace_id                                                            AS ultimate_parent_namespace_id,
      NULL                                                                                    AS license_subscription_id,
      stage_name                                                                              AS stage_name,
      section_name                                                                            AS section_name,
      group_name                                                                              AS group_name,
      NULL                                                                                    AS edition,
      TO_NUMBER(NULL)                                                                         AS major_version,
      TO_NUMBER(NULL)                                                                         AS minor_version,
      NULL                                                                                    AS usage_ping_delivery_type,
      'GITLAB_DOTCOM'                                                                         AS event_source
    FROM gitlab_dotcom

), flattened_usage AS (

    SELECT
      payload.dim_usage_ping_id                                                               AS dim_usage_ping_id,
      payload.path                                                                            AS metrics_path,
      payload.dim_product_tier_id                                                             AS dim_product_tier_id,
      payload.dim_subscription_id                                                             AS dim_subscription_id,
      payload.dim_location_country_id                                                         AS dim_location_country_id,
      payload.dim_date_id                                                                     AS dim_date_id,
      payload.dim_instance_id                                                                 AS dim_instance_id,
      payload.ping_created_at                                                                 AS ping_created_at,
      payload.ping_created_at_date                                                            AS ping_created_at_date,
      payload.edition                                                                         AS edition,
      payload.product_tier                                                                    AS product_tier,
      payload.major_version                                                                   AS major_version,
      payload.minor_version                                                                   AS minor_version,
      payload.usage_ping_delivery_type                                                        AS usage_ping_delivery_type,
      payload.is_internal                                                                     AS is_internal,
      payload.is_staging                                                                      AS is_staging,
      payload.is_trial                                                                        AS is_trial,
      payload.instance_user_count                                                             AS instance_user_count,
      payload.host_name                                                                       AS host_name,
      payload.umau_value                                                                      AS umau_value,
      payload.license_subscription_id                                                         AS license_subscription_id,
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
      dim_crm_opportunity_id                                                                  AS dim_crm_opportunity_id,
      dim_subscription_id_original                                                            AS dim_subscription_id_original,
      namespace_id                                                                            AS namespace_id
    FROM flattened_w_metrics
    LEFT JOIN dim_subscription
      ON flattened_w_metrics.dim_subscription_id = dim_subscription.dim_subscription_id

), usage_ping_fact AS (

    SELECT
      dim_instance_id                                                                         AS dim_instance_id,
      TO_NUMBER(NULL)                                                                         AS dim_namespace_id,
      {{ dbt_utils.surrogate_key(['dim_usage_ping_id', 'metrics_path']) }}                    AS event_id,
      event_name                                                                              AS event_name,
      TO_NUMBER(event_count)                                                                  AS event_count,
      TO_NUMBER(dim_date_id)                                                                  AS dim_event_date_id,
      dim_usage_ping_id                                                                       AS dim_usage_ping_id,
      metrics_path                                                                            AS metrics_path,
      dim_product_tier_id                                                                     AS dim_product_tier_id,
      dim_subscription_id                                                                     AS dim_subscription_id,
      dim_location_country_id                                                                 AS dim_location_country_id,
      dim_crm_account_id                                                                      AS dim_crm_account_id,
      dim_billing_account_id                                                                  AS dim_billing_account_id,
      dim_crm_opportunity_id                                                                  AS dim_crm_opportunity_id,
      dim_subscription_id_original                                                            AS dim_subscription_id_original,
      TO_NUMBER(NULL)                                                                         AS ultimate_parent_namespace_id,
      license_subscription_id                                                                 AS license_subscription_id,
      stage_name                                                                              AS stage_name,
      section_name                                                                            AS section_name,
      group_name                                                                              AS group_name,
      edition                                                                                 AS edition,
      major_version                                                                           AS major_version,
      minor_version                                                                           AS minor_version,
      usage_ping_delivery_type                                                                AS usage_ping_delivery_type,
      'SERVICE PINGS'                                                                         AS event_source
    FROM flattened_w_subscription
    {{ dbt_utils.group_by(n=25) }}

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
    updated_date="2022-02-01"
) }}
