{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('rpt_service_ping_instance_metric_adoption_subscription_monthly', 'rpt_service_ping_instance_metric_adoption_subscription_monthly'),
    ('rpt_service_ping_instance_metric_adoption_subscription_metric_monthly', 'rpt_service_ping_instance_metric_adoption_subscription_metric_monthly')
    ])

}}

-- union the two adoption methods to pipe into estimations model

, final AS (

  SELECT
      {{ dbt_utils.surrogate_key(['reporting_month', 'metrics_path', 'estimation_grain']) }}          AS rpt_service_ping_instance_metric_adoption_monthly_all_id,
      {{ dbt_utils.star(from=ref('rpt_service_ping_instance_metric_adoption_subscription_monthly'), except=['RPT_SERVICE_PING_INSTANCE_METRIC_ADOPTION_SUBSCRIPTION_MONTHLY_ID', 'CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE', 'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }}
  FROM rpt_service_ping_instance_metric_adoption_subscription_monthly

  UNION ALL

  SELECT
      {{ dbt_utils.surrogate_key(['reporting_month', 'metrics_path', 'estimation_grain']) }}          AS rpt_service_ping_instance_metric_adoption_monthly_all_id,
      {{ dbt_utils.star(from=ref('rpt_service_ping_instance_metric_adoption_subscription_metric_monthly'), except=['RPT_SERVICE_PING_INSTANCE_METRIC_ADOPTION_SUBSCRIPTION_METRIC_MONTHLY_ID', 'CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE', 'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }}
  FROM rpt_service_ping_instance_metric_adoption_subscription_metric_monthly

)

 {{ dbt_audit(
     cte_ref="final",
     created_by="@icooper-acp",
     updated_by="@icooper-acp",
     created_date="2022-04-20",
     updated_date="2022-04-20"
 ) }}
