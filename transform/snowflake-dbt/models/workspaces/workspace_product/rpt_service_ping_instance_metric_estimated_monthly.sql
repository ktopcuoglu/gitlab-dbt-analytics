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
        dim_date_id,
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
        WHERE latest_active_subscription_id IS NOT NULL
            AND is_last_ping_of_month = TRUE
            AND service_ping_delivery_type = 'Self-Managed'
            AND has_timed_out = FALSE


), fact_w_month AS (

    SELECT
        metrics_path                      AS metrics_path,
        dim_date.first_day_of_month       AS month,
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
            ON fact.dim_date_id = dim_date.date_id
    WHERE metric_value is not null
    {{ dbt_utils.group_by(n=12)}}

), joined_counts_w_percentage AS (

  SELECT
      fact_w_month.*,
      mart_pct.active_count         AS active_count,
      mart_pct.inactive_count       AS inactive_count,
      mart_pct.pct_with_counters    AS pct_with_counters,
      mart_pct.adoption_grain       AS adoption_grain
    FROM fact_w_month
      LEFT JOIN mart_pct
    ON fact_w_month.month = mart_pct.month
      AND fact_w_month.metrics_path = mart_pct.metrics_path

), final AS (

SELECT
    {{ dbt_utils.surrogate_key(['month', 'metrics_path']) }}                                                                          AS rpt_service_ping_instance_metric_estimated_monthly_id,
    *,
    {{ usage_estimation('metric_value', 'pct_with_counters') }}                                                                       AS estimated_usage
 FROM joined_counts_w_percentage

)

 {{ dbt_audit(
     cte_ref="final",
     created_by="@icooper-acp",
     updated_by="@icooper-acp",
     created_date="2022-04-07",
     updated_date="2022-04-07"
 ) }}
