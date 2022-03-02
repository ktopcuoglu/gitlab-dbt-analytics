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
    ('dim_subscription', 'dim_subscription'),
    ('dim_namespace', 'dim_namespace')
    ])

}}

, fct_events AS  (

    SELECT
      event_primary_key                                               AS event_primary_key,
      usage_data_events.event_name                                    AS event_name,
      namespace_id                                                    AS namespace_id, -- make empty namespace = null
      'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'                          AS dim_instance_id,
      user_id                                                         AS user_id,
      parent_type                                                     AS parent_type,
      parent_id                                                       AS parent_id,
      IFF(usage_data_events.parent_type = 'project', parent_id, NULL) AS project_id,
      event_created_at                                                AS event_created_at,
      CASE
          WHEN usage_data_events.stage_name IS NULL
            THEN xmau_metrics.stage_name
          ELSE usage_data_events.stage_name
         end                                                        AS stage_name,
      group_name                                                    AS group_name,
      section_name                                                  AS section_name,
      smau                                                          AS smau,
      gmau                                                          AS gmau,
      is_umau                                                       AS umau,
      project_is_learn_gitlab                                       AS project_is_learn_gitlab
    FROM usage_data_events
    LEFT JOIN xmau_metrics
      ON usage_data_events.event_name = xmau_metrics.events_to_include

), paid_flag_by_day AS (

    SELECT
        namespace_id,
        CAST(event_created_at AS DATE)                                  AS event_date,
        plan_was_paid_at_event_date                                     AS plan_was_paid_at_event_date,
        plan_id_at_event_date                                           AS plan_id_at_event_date,
        plan_name_at_event_date                                         AS plan_name_at_event_date,
        event_created_at
    FROM usage_data_events
        QUALIFY ROW_NUMBER() OVER (PARTITION BY namespace_id, event_date
                                   ORDER BY event_created_at DESC) = 1

), fct_events_w_plan_was_paid AS (

  SELECT
    fct_events.*,
    paid_flag_by_day.plan_was_paid_at_event_date                     AS plan_was_paid_at_event_date,
    paid_flag_by_day.plan_id_at_event_date                           AS plan_id_at_event_date,
    paid_flag_by_day.plan_name_at_event_date                         AS plan_name_at_event_date
  FROM fct_events
    LEFT JOIN paid_flag_by_day
  ON fct_events.namespace_id = paid_flag_by_day.namespace_id and CAST(fct_events.event_created_at AS DATE) = paid_flag_by_day.event_date

), deduped_namespace_bdg AS (

  SELECT
    bdg.dim_subscription_id                   AS dim_subscription_id,
    bdg.order_id                              AS order_id,
    bdg.dim_crm_account_id                    AS dim_crm_account_id,
    bdg.dim_billing_account_id                AS dim_billing_account_id,
    bdg.dim_namespace_id                      AS dim_namespace_id
        FROM namespace_order_subscription AS bdg
    INNER JOIN
        dim_subscription AS ds
    ON bdg.dim_subscription_id = ds.dim_subscription_id
    WHERE product_tier_name_subscription IN ('SaaS - Bronze', 'SaaS - Ultimate', 'SaaS - Premium')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_namespace_id ORDER BY subscription_version DESC) = 1

), dim_namespace_w_bdg AS (

    SELECT
      dim_namespace.dim_namespace_id                            AS dim_namespace_id,
      dim_namespace.dim_product_tier_id                         AS dim_product_tier_id,
      deduped_namespace_bdg.dim_subscription_id                 AS dim_subscription_id,
      deduped_namespace_bdg.order_id                            AS order_id,
      deduped_namespace_bdg.dim_crm_account_id                  AS dim_crm_account_id,
      deduped_namespace_bdg.dim_billing_account_id              AS dim_billing_account_id
    FROM deduped_namespace_bdg
    INNER JOIN dim_namespace
      ON dim_namespace.dim_namespace_id = deduped_namespace_bdg.dim_namespace_id

), final AS (

    SELECT *
    FROM fct_events_w_plan_was_paid
    LEFT JOIN dim_namespace_w_bdg
      ON fct_events_w_plan_was_paid.namespace_id = dim_namespace_w_bdg.dim_namespace_id

), gitlab_dotcom_fact AS (

    SELECT
      event_primary_key                       AS event_id,
      event_name                              AS event_name,
      dim_instance_id                         AS dim_instance_id,
      dim_product_tier_id                     AS dim_product_tier_id,
      dim_subscription_id                     AS dim_subscription_id,
      date_id                                 AS dim_event_date_id,
      dim_crm_account_id                      AS dim_crm_account_id,
      dim_billing_account_id                  AS dim_billing_account_id,
      namespace_id                            AS dim_namespace_id,
      project_id                              AS dim_project_id,
      user_id                                 AS dim_user_id,
      stage_name                              AS stage_name,
      section_name                            AS section_name,
      group_name                              AS group_name,
      event_created_at                        AS event_created_at,
      plan_id_at_event_date                   AS plan_id_at_event_date,
      plan_name_at_event_date                 AS plan_name_at_event_date,
      plan_was_paid_at_event_date             AS plan_was_paid_at_event_date,
      project_is_learn_gitlab                 AS project_is_learn_gitlab,
      'GITLAB_DOTCOM'                         AS data_source
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
    updated_date="2022-02-10"
) }}
