{{ simple_cte([
    ('mart_monthly_product_usage','mart_monthly_product_usage'),
    ('mart_usage_ping_counters_statistics','mart_usage_ping_counters_statistics'),
    ('gitlab_release_schedule','gitlab_release_schedule'),
    ('mart_paid_subscriptions_monthly_usage_ping_optin','mart_paid_subscriptions_monthly_usage_ping_optin'),
    ('wk_usage_ping_monthly_events_distribution_by_version','wk_usage_ping_monthly_events_distribution_by_version')
]) }}

, cte_joined AS (

    SELECT 
      reporting_month, 
      stage_name,
      IFF(main_edition='CE', main_edition, IFF(ping_product_tier = 'Core', 'EE - Core', 'EE - Paid')) AS reworked_main_edition,
      DATEDIFF('month', DATE_TRUNC('month', release_date), reporting_month) AS months_since_release, 
      SUM(monthly_metric_value) AS month_metric_value_sum
    FROM mart_monthly_product_usage
    LEFT JOIN mart_usage_ping_counters_statistics
      ON mart_monthly_product_usage.main_edition = mart_usage_ping_counters_statistics.edition
      AND mart_monthly_product_usage.metrics_path = mart_usage_ping_counters_statistics.metrics_path
    LEFT JOIN gitlab_release_schedule
      ON gitlab_release_schedule.major_minor_version = mart_usage_ping_counters_statistics.first_version_with_counter
    WHERE is_smau = TRUE
      AND delivery = 'Self-Managed'
    GROUP BY 1,2,3,4
  
), pct_of_instances AS (

    SELECT 
      cte_joined.reporting_month, 
      month_metric_value_sum, 
      stage_name,
      cte_joined.reworked_main_edition,
      SUM(CASE WHEN distrib.months_since_release <= cte_joined.months_since_release THEN total_counts END) / SUM(total_counts) AS pct_of_instances
    FROM cte_joined
    LEFT JOIN wk_usage_ping_monthly_events_distribution_by_version  AS distrib 
      ON cte_joined.reporting_month = distrib.reporting_month
        AND distrib.reworked_main_edition = cte_joined.reworked_main_edition
    GROUP BY 1,2,3,4
      
), averaged AS (
  
    SELECT *
    FROM pct_of_instances

), opt_in_rate AS (
  
  SELECT 
    reporting_month,
    AVG(has_sent_payloads::INTEGER) AS opt_in_rate
  FROM mart_paid_subscriptions_monthly_usage_ping_optin
  GROUP BY 1 
)

SELECT 
  pct_of_instances.reporting_month::DATE AS reporting_month,
  stage_name,
  reworked_main_edition,
  IFF(reworked_main_edition = 'CE', 'CE', 'EE') AS main_edition,
  pct_of_instances,
  month_metric_value_sum,
  month_metric_value_sum / pct_of_instances / opt_in_rate AS estimated_xmau
FROM pct_of_instances
LEFT JOIN opt_in_rate
  ON pct_of_instances.reporting_month = opt_in_rate.reporting_month
