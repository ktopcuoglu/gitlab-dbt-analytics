{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_namespace', 'dim_namespace'),
    ('fct_event_usage_metrics', 'fct_event_usage_metrics'),
    ('xmau_metrics', 'gitlab_dotcom_xmau_metrics'),
    ])
}}

, fact_table_source AS (
    SELECT *
    FROM fct_event_usage_metrics

), fact_with_date AS (

    SELECT
      fact.event_id,
      CAST(fact.event_created_at AS DATE)                          AS event_date,
      fact.user_id,
      fact.event_name,
      fact.dim_product_tier_id,
      fact.dim_subscription_id,
      fact.dim_crm_account_id,
      fact.dim_billing_account_id,
      fact.stage_name,
      fact.section_name,
      fact.group_name,
      fact.source,
      fact.plan_id_at_event_date,
      fact.plan_name_at_event_date,
      fact.plan_was_paid_at_event_date,
      fact.dim_namespace_id
    FROM fact_table_source AS fact

), fact_with_namespace AS (

    SELECT
        fact.*,
        CAST(namespace.created_at AS DATE)                         AS namespace_created_at,
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

), results AS (

  SELECT
    *
  FROM fact_with_xmau_flags

)

{{ dbt_audit(
    cte_ref="results",
    created_by="@dihle",
    updated_by="@dihle",
    created_date="2022-01-28",
    updated_date="2022-02-07"
) }}
