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
      TO_DATE(event_created_at)                                    AS event_date,
      dim_user_id,
      event_name,
      dim_product_tier_id,
      dim_subscription_id,
      dim_crm_account_id,
      dim_billing_account_id,
      stage_name,
      section_name,
      group_name,
      data_source,
      plan_id_at_event_date,
      plan_name_at_event_date,
      plan_was_paid_at_event_date,
      dim_namespace_id,
      dim_instance_id
    FROM fct_usage_event

), fact_with_namespace AS (

    SELECT
        fact.*,
        TO_DATE(namespace.created_at)                              AS namespace_created_at,
        DATEDIFF(day, namespace_created_at,GETDATE())              AS days_since_namespace_created
    FROM fact_with_date as fact
    LEFT JOIN dim_namespace as namespace
        ON fact.dim_namespace_id = namespace.dim_namespace_id

), fact_with_xmau_flags AS (
    SELECT
        fact.*,
        xmau.smau                                                  AS is_smau,
        xmau.gmau                                                  AS is_gmau,
        xmau.is_umau                                               AS is_umau
    FROM fact_with_namespace AS fact
    LEFT JOIN xmau_metrics AS xmau
        ON fact.event_name = xmau.events_to_include

), results AS (

    SELECT *
    FROM fact_with_xmau_flags

)

{{ dbt_audit(
    cte_ref="results",
    created_by="@dihle",
    updated_by="@dihle",
    created_date="2022-01-28",
    updated_date="2022-02-09"
) }}
