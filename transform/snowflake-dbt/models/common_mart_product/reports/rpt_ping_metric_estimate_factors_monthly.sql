{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('rpt_ping_subscriptions_reported_estimate_factors_monthly', 'rpt_ping_subscriptions_reported_estimate_factors_monthly'),
    ('rpt_ping_subscriptions_on_versions_estimate_factors_monthly', 'rpt_ping_subscriptions_on_versions_estimate_factors_monthly')
    ])

}}

-- union the two adoption methods to pipe into estimations model

, final AS (

  SELECT
      {{ dbt_utils.surrogate_key(['ping_created_at_month', 'metrics_path', 'ping_edition', 'estimation_grain']) }}          AS ping_metric_estimate_factors_monthly_id,
      {{ dbt_utils.star(from=ref('rpt_ping_subscriptions_reported_estimate_factors_monthly'), except=['PING_SUBSCRIPTIONS_REPORTED_ESTIMATE_FACTORS_MONTHLY_ID', 'CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE', 'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }}
  FROM rpt_ping_subscriptions_reported_estimate_factors_monthly

  UNION ALL

  SELECT
      {{ dbt_utils.surrogate_key(['ping_created_at_month', 'metrics_path', 'ping_edition', 'estimation_grain']) }}          AS ping_metric_estimate_factors_monthly_id,
      {{ dbt_utils.star(from=ref('rpt_ping_subscriptions_on_versions_estimate_factors_monthly'), except=['PING_SUBSCRIPTIONS_ON_VERSIONS_ESTIMATE_FACTORS_MONTHLY_ID', 'CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE', 'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }}
  FROM rpt_ping_subscriptions_on_versions_estimate_factors_monthly

)

 {{ dbt_audit(
     cte_ref="final",
     created_by="@icooper-acp",
     updated_by="@snalamaru",
     created_date="2022-04-20",
     updated_date="2022-06-07"
 ) }}
