{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('mart_service_ping_instance_metric_28_day', 'mart_service_ping_instance_metric_28_day'),
    ('mart_pct', 'rpt_service_ping_instance_metric_adoption_monthly_all')
    ])

}}

-- Fact data from mart_service_ping_instance_metric_28_day, bringing in only last ping of months which are valid

, fact AS (

    SELECT
      metrics_path                      AS metrics_path,
      ping_created_at_month             AS reporting_month,
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
      SUM(metric_value)                 AS actual_usage
    FROM mart_service_ping_instance_metric_28_day
        WHERE is_last_ping_of_month = TRUE
            AND has_timed_out = FALSE
            AND metric_value is not null
    {{ dbt_utils.group_by(n=13)}}

--Join in adoption percentages to determine what to estimate for self managed

), sm_joined_counts_w_percentage AS (

  SELECT
      fact.*,
      mart_pct.reporting_count          AS reporting_count,
      mart_pct.no_reporting_count       AS no_reporting_count,
      mart_pct.percent_reporting        AS percent_reporting,
      mart_pct.estimation_grain         AS estimation_grain
    FROM fact
      INNER JOIN mart_pct
    ON fact.reporting_month = mart_pct.reporting_month
      AND fact.metrics_path = mart_pct.metrics_path
  WHERE service_ping_delivery_type = 'Self-Managed'

-- No need to join in SaaS, it is what it is (all reported and accurate)

), saas_joined_counts_w_percentage AS (

  SELECT
      fact.*,
      1                                         AS reporting_count,
      0                                         AS no_reporting_count,
      1                                         AS percent_reporting,
      'SaaS'    AS estimation_grain
    FROM fact
  WHERE service_ping_delivery_type = 'SaaS'

-- Union SaaS and Self Managed tables

), joined_counts_w_percentage AS (

  SELECT * FROM saas_joined_counts_w_percentage

  UNION ALL

  SELECT * FROM sm_joined_counts_w_percentage

--Format output

), final AS (

SELECT
    {{ dbt_utils.surrogate_key(['reporting_month', 'metrics_path', 'estimation_grain', 'ping_edition_product_tier', 'service_ping_delivery_type']) }}   AS rpt_service_ping_instance_metric_estimated_monthly_id,
    -- identifiers
    metrics_path                                                                                                                                        AS metrics_path,
    reporting_month                                                                                                                                     AS reporting_month,
    service_ping_delivery_type                                                                                                                          AS service_ping_delivery_type,
    -- ping attributes
    ping_edition                                                                                                                                        AS ping_edition,
    ping_product_tier                                                                                                                                   AS ping_product_tier,
    ping_edition_product_tier                                                                                                                           AS ping_edition_product_tier,
    -- metric attributes
    stage_name                                                                                                                                          AS stage_name,
    section_name                                                                                                                                        AS section_name,
    group_name                                                                                                                                          AS group_name,
    is_smau                                                                                                                                             AS is_smau,
    is_gmau                                                                                                                                             AS is_gmau,
    is_paid_gmau                                                                                                                                        AS is_paid_gmau,
    is_umau                                                                                                                                             AS is_umau,
    -- fct info
    reporting_count                                                                                                                                     AS reporting_count,
    no_reporting_count                                                                                                                                  AS no_reporting_count,
    percent_reporting                                                                                                                                   AS percent_reporting,
    estimation_grain                                                                                                                                    AS estimation_grain,
    {{ usage_estimation('actual_usage', 'percent_reporting') }}                                                                                         AS total_usage_estimated,
    total_usage_estimated - actual_usage                                                                                                                AS estimated_usage,
    actual_usage                                                                                                                                        AS actual_usage
 FROM joined_counts_w_percentage

)

 {{ dbt_audit(
     cte_ref="final",
     created_by="@icooper-acp",
     updated_by="@icooper-acp",
     created_date="2022-04-20",
     updated_date="2022-04-20"
 ) }}
