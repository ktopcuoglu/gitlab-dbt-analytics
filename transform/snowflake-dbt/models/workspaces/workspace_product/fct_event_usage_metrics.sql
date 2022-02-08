{{ config(
    tags=["mnpi_exception"]
) }}

{{config({
    "materialized": "table"
  })
}}

{{ simple_cte([
    ('xmau_metrics', 'gitlab_dotcom_xmau_metrics'),
    ('usage_data_events', 'gitlab_dotcom_usage_data_events'),
    ('dim_usage_ping_metric', 'dim_usage_ping_metric'),
    ('dim_license', 'dim_license'),
    ('dim_date', 'dim_date'),
    ('namespace_order_subscription', 'bdg_namespace_order_subscription'),
    ('dim_namespace', 'dim_namespace')
    ])

}}

, fct_events AS  (

    SELECT
      event_primary_key                                             AS event_primary_key,
      usage_data_events.event_name                                  AS event_name,
      namespace_id                                                  AS namespace_id,
      user_id                                                       AS user_id,
      parent_type                                                   AS parent_type,
      parent_id                                                     AS parent_id,
      event_created_at                                              AS event_created_at,
      plan_id_at_event_date                                         AS plan_id_at_event_date,
      plan_name_at_event_date                                       AS plan_name_at_event_date,
      plan_was_paid_at_event_date                                   AS plan_was_paid_at_event_date,
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
    LEFT JOIN xmau_metrics
      ON usage_data_events.event_name = xmau_metrics.events_to_include

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
      dim_product_tier_id                     AS dim_product_tier_id,
      dim_subscription_id                     AS dim_subscription_id,
      date_id                                 AS dim_event_date_id,
      dim_crm_account_id                      AS dim_crm_account_id,
      dim_billing_account_id                  AS dim_billing_account_id,
      dim_namespace_id                        AS dim_namespace_id,
      user_id                                 AS user_id,
      stage_name                              AS stage_name,
      section_name                            AS section_name,
      group_name                              AS group_name,
      event_created_at                        AS event_created_at,
      plan_id_at_event_date                   AS plan_id_at_event_date,
      plan_name_at_event_date                 AS plan_name_at_event_date,
      plan_was_paid_at_event_date             AS plan_was_paid_at_event_date,
      'GITLAB_DOTCOM'                         AS source
    FROM final
    LEFT JOIN dim_date
      ON TO_DATE(event_created_at) = dim_date.date_day

), results AS (

    SELECT *
    FROM gitlab_dotcom_fact

)

{{ dbt_audit(
    cte_ref="results",
    created_by="@icooper-acp",
    updated_by="@icooper-acp",
    created_date="2022-01-20",
    updated_date="2022-01-24"
) }}
