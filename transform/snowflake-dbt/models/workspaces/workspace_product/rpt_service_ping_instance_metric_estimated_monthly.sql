{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('mart_service_ping_instance_metric_28_day', 'mart_service_ping_instance_metric_28_day'),
    ('mart_pct', 'rpt_service_ping_instance_metric_adoption_monthly')
    ])

}}

, fact AS (

    SELECT
        metrics_path,
        metric_value,
        latest_active_subscription_id,
        service_ping_delivery_type,
        dim_service_ping_date_id,
        ping_edition,
        ping_product_tier,
        ping_edition_product_tier,
        stage_name,
        section_name,
        group_name,
        is_smau,
        is_gmau,
        is_paid_gmau,
        is_umau
    FROM mart_service_ping_instance_metric_28_day
        WHERE is_last_ping_of_month = TRUE
            --AND service_ping_delivery_type = 'Self-Managed'
            AND has_timed_out = FALSE


), fact_w_month AS (

    SELECT
        metrics_path                      AS metrics_path,
        dim_date.first_day_of_month       AS reporting_month,
        service_ping_delivery_type        AS service_ping_delivery_type,
        ping_edition                      AS ping_edition,
        ping_product_tier                 AS ping_product_tier,
        ping_edition_product_tier         AS ping_edition_product_tier,
        stage_name                        AS stage_name,
        section_name                      AS section_name,
        group_name                        AS group_name,
        is_smau                           AS is_smau,
        is_gmau                           AS is_gmau,
        is_paid_gmau                      AS is_paid_gmau,
        is_umau                           AS is_umau,
        SUM(metric_value)                 AS metric_value
    FROM fact
        INNER JOIN dim_date
            ON fact.dim_service_ping_date_id = dim_date.date_id
    WHERE metric_value is not null
    {{ dbt_utils.group_by(n=13)}}

), sm_joined_counts_w_percentage AS (

  SELECT
      fact_w_month.*,
      mart_pct.reporting_count          AS reporting_count,
      mart_pct.no_reporting_count       AS no_reporting_count,
      mart_pct.pct_with_counters        AS pct_with_counters,
      mart_pct.estimation_grain         AS estimation_grain
    FROM fact_w_month
      LEFT JOIN mart_pct
    ON fact_w_month.reporting_month = mart_pct.reporting_month
      AND fact_w_month.metrics_path = mart_pct.metrics_path
  WHERE service_ping_delivery_type = 'Self-Managed'

), saas_joined_counts_w_percentage AS (

  SELECT
      fact_w_month.*,
      1                                 AS reporting_count,
      0                                 AS no_reporting_count,
      1                                 AS pct_with_counters,
      'subscription based estimation'   AS estimation_grain
    FROM fact_w_month
  WHERE service_ping_delivery_type = 'SaaS'

  UNION ALL

  SELECT
      fact_w_month.*,
      1                                 AS reporting_count,
      0                                 AS no_reporting_count,
      1                                 AS pct_with_counters,
      'seat based estimation'           AS estimation_grain
    FROM fact_w_month
  WHERE service_ping_delivery_type = 'SaaS'

), joined_counts_w_percentage AS (

  SELECT * FROM saas_joined_counts_w_percentage

  UNION ALL

  SELECT * FROM sm_joined_counts_w_percentage

), final AS (

SELECT
    {{ dbt_utils.surrogate_key(['reporting_month', 'metrics_path', 'estimation_grain', 'ping_edition_product_tier']) }}   AS rpt_service_ping_instance_metric_estimated_monthly_id,
    *,
    {{ usage_estimation('metric_value', 'pct_with_counters') }}                                                           AS estimated_usage
 FROM joined_counts_w_percentage

)

 {{ dbt_audit(
     cte_ref="final",
     created_by="@icooper-acp",
     updated_by="@icooper-acp",
     created_date="2022-04-07",
     updated_date="2022-04-07"
 ) }}
