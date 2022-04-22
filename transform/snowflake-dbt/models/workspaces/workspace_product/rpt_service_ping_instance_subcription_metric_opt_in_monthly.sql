{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('metric_opt_in', 'rpt_service_ping_counter_statistics'),
    ('mart_arr', 'mart_arr'),
    ('mart_service_ping_instance_metric_28_day', 'mart_service_ping_instance_metric_28_day')
    ])

}}

/*
Determine latest version for each subscription to determine if the potential metric is valid for a given month
*/

, subscriptions_w_versions AS (

  SELECT
      ping_created_at_month             AS ping_created_at_month,
      dim_service_ping_instance_id      AS dim_service_ping_instance_id,
      latest_active_subscription_id     AS latest_active_subscription_id,
      ping_edition                      AS ping_edition,
      major_minor_version               AS major_minor_version,
      instance_user_count               AS instance_user_count
  FROM mart_service_ping_instance_metric_28_day
      WHERE is_last_ping_of_month = TRUE
        AND service_ping_delivery_type = 'Self-Managed'
        AND ping_product_tier != 'Storage'
        AND latest_active_subscription_id IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ping_created_at_month, latest_active_subscription_id, dim_service_ping_instance_id
              ORDER BY major_minor_version_id DESC) = 1

/*
Grab just the metrics relevant to the subscription based upon version
*/

), active_subscriptions_by_metric AS (

  SELECT
    subscriptions_w_versions.*,
    metric_opt_in.metrics_path                                          AS metrics_path,
    metric_opt_in.first_version_with_counter                            AS first_version_with_counter,
    metric_opt_in.last_version_with_counter                             AS last_version_with_counter
  FROM subscriptions_w_versions
    INNER JOIN metric_opt_in
      ON subscriptions_w_versions.major_minor_version
        BETWEEN metric_opt_in.first_version_with_counter AND metric_opt_in.last_version_with_counter
        AND subscriptions_w_versions.ping_edition = metric_opt_in.ping_edition

), arr_counts_joined AS (

  SELECT
    active_subscriptions_by_metric.*,
    quantity
  FROM active_subscriptions_by_metric
    INNER JOIN mart_arr
  ON active_subscriptions_by_metric.latest_active_subscription_id = mart_arr.dim_subscription_id
      AND active_subscriptions_by_metric.ping_created_at_month = mart_arr.arr_month

/*
Aggregate for subscription and user counters
*/

), agg_subscriptions AS (

SELECT
    {{ dbt_utils.surrogate_key(['ping_created_at_month', 'metrics_path']) }}          AS rpt_service_ping_instance_subcription_metric_opt_in_monthly_id,
    ping_created_at_month                                                             AS arr_month,
    metrics_path                                                                      AS metrics_path,
    COUNT(latest_active_subscription_id)                                              AS total_subscription_count,
    SUM(quantity)                                                                     AS total_licensed_users
FROM arr_counts_joined
    {{ dbt_utils.group_by(n=3)}}

)

 {{ dbt_audit(
     cte_ref="agg_subscriptions",
     created_by="@icooper-acp",
     updated_by="@icooper-acp",
     created_date="2022-04-20",
     updated_date="2022-04-20"
 ) }}
