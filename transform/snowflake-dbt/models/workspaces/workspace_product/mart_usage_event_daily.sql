{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_namespace', 'dim_namespace'),
    ('fct_usage_event', 'fct_usage_event'),
    ('xmau_metrics', 'gitlab_dotcom_xmau_metrics'),
    ])
}}

, fact_with_date AS (
    SELECT
      event_id,
      CAST(event_created_at AS DATE)                                      AS event_date,
      dim_user_id                                                         AS dim_user_id,
      event_name,
      plan_id_at_event_date,
      {{ dbt_utils.surrogate_key(['event_date', 'dim_user_id',
        'dim_namespace_id',
        'event_name']) }}                                                 AS mart_usage_event_daily_id,
      dim_product_tier_id,
      dim_subscription_id,
      dim_crm_account_id,
      dim_billing_account_id,
      stage_name,
      section_name,
      group_name,
      data_source,
      plan_name_at_event_date,
      plan_was_paid_at_event_date,
      dim_namespace_id,
      dim_instance_id
    FROM fct_usage_event AS fact

), fact_with_namespace AS (

    SELECT
      fact.*,
      CAST(namespace.created_at AS DATE)                                  AS namespace_created_at,
      DATEDIFF(DAY, namespace_created_at,GETDATE())                       AS days_since_namespace_created,
      DATEDIFF(DAY, namespace_created_at, event_date)                     AS days_since_namespace_creation_at_event_date,
      namespace.namespace_is_internal                                     AS namespace_is_internal
    FROM fact_with_date AS fact
    LEFT JOIN dim_namespace AS namespace
        ON fact.dim_namespace_id = namespace.dim_namespace_id

), fact_with_xmau_flags AS (

    SELECT
      fact.*,
      xmau.smau                                                           AS is_smau,
      xmau.gmau                                                           AS is_gmau,
      xmau.is_umau                                                        AS is_umau
    FROM fact_with_namespace AS fact
    LEFT JOIN xmau_metrics AS xmau
        ON fact.event_name = xmau.events_to_include

), results AS (

    SELECT
      mart_usage_event_daily_id,
      event_date,
      dim_user_id,
      dim_namespace_id,
      dim_instance_id,
      plan_id_at_event_date,
      event_name,
      dim_product_tier_id,
      dim_subscription_id,
      dim_crm_account_id,
      dim_billing_account_id,
      stage_name,
      section_name,
      group_name,
      data_source,
      plan_name_at_event_date,
      plan_was_paid_at_event_date,
      namespace_is_internal,
      namespace_created_at,
      days_since_namespace_created,
      days_since_namespace_creation_at_event_date,
      is_smau,
      is_gmau,
      is_umau,
      COUNT(*)                                                            AS event_count
    FROM fact_with_xmau_flags
    {{ dbt_utils.group_by(n=24) }}

)

{{ dbt_audit(
    cte_ref="results",
    created_by="@dihle",
    updated_by="@dihle",
    created_date="2022-02-11",
    updated_date="2022-02-15"
) }}
