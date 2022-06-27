{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('metric_opt_in', 'rpt_ping_metric_first_last_versions'),
    ('latest_subscriptions', 'rpt_ping_latest_subscriptions_monthly')
    ])

}}

/*
Attach metrics_path to subscription IF the subscription is on a version which it can report on
*/

, latest_subscriptions_by_metric AS (

  SELECT
    latest_subscriptions.ping_created_at_month                          AS ping_created_at_month,
    latest_subscriptions.arr                                            AS arr,
    latest_subscriptions.latest_subscription_id                         AS latest_subscription_id,
    latest_subscriptions.licensed_user_count                            AS licensed_user_count,
    metric_opt_in.ping_edition                                          AS ping_edition,
    metric_opt_in.metrics_path                                          AS metrics_path
  FROM latest_subscriptions
    INNER JOIN metric_opt_in
      ON latest_subscriptions.major_minor_version_id
        BETWEEN metric_opt_in.first_major_minor_version_id_with_counter AND metric_opt_in.last_major_minor_version_id_with_counter
        AND latest_subscriptions.ping_edition = metric_opt_in.ping_edition
      {{ dbt_utils.group_by(n=6)}}
/*
Aggregate CTE to determine count of arr, subscriptions and licensed users for each month/metric.
*/

), agg_subscriptions AS (

SELECT
    {{ dbt_utils.surrogate_key(['ping_created_at_month', 'metrics_path', 'ping_edition']) }}          AS ping_subscriptions_on_versions_counts_monthly_id,
    ping_created_at_month                                                                             AS ping_created_at_month,
    metrics_path                                                                                      AS metrics_path,
    ping_edition                                                                                      AS ping_edition,
    SUM(arr)                                                                                          AS total_arr,
    COUNT(DISTINCT latest_subscription_id)                                                            AS total_subscription_count,
    SUM(licensed_user_count)                                                                          AS total_licensed_users
FROM latest_subscriptions_by_metric
    {{ dbt_utils.group_by(n=4)}}

)

 {{ dbt_audit(
     cte_ref="agg_subscriptions",
     created_by="@icooper-acp",
     updated_by="@snalamaru",
     created_date="2022-04-20",
     updated_date="2022-06-08"
 ) }}
