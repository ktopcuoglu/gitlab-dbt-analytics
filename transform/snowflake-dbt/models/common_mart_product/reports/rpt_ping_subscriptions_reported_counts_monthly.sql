{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
  ('latest_subscriptions', 'rpt_ping_latest_subscriptions_monthly'),
  ('mart_ping_instance_metric', 'mart_ping_instance_metric')
    ])

}}

-- Determine monthly sub and user count

, dedupe_latest_subscriptions AS (

  SELECT
    ping_created_at_month                                             AS ping_created_at_month,
    --ping_edition                                                      AS ping_edition,
    latest_subscription_id                                            AS latest_subscription_id,
    licensed_user_count                                               AS licensed_user_count,
    arr                                                               AS arr
  FROM latest_subscriptions
    {{ dbt_utils.group_by(n=4)}}

), subscription_info AS (

  SELECT
    ping_created_at_month                                             AS ping_created_at_month,
    --ping_edition                                                      AS ping_edition,
    1                                                                 AS key,
    SUM(arr)                                                          AS total_arr,
    COUNT(DISTINCT latest_subscription_id)                            AS total_subscription_count,
    SUM(licensed_user_count)                                          AS total_licensed_users
  FROM dedupe_latest_subscriptions
      {{ dbt_utils.group_by(n=2)}}

), metrics AS (

    SELECT --grab all metrics and editions reported on a given month
      ping_created_at_month,
      metrics_path,
      ping_edition,
      1 AS key
    FROM mart_ping_instance_metric
    GROUP BY 1, 2, 3, 4


-- Join to get combo of all possible subscriptions and the metrics

), sub_combo AS (

    SELECT
      {{ dbt_utils.surrogate_key(['subscription_info.ping_created_at_month', 'metrics.metrics_path', 'metrics.ping_edition']) }}          AS ping_subscriptions_reported_counts_monthly_id,
      subscription_info.ping_created_at_month                                                                                             AS ping_created_at_month,
      metrics.metrics_path                                                                                                                AS metrics_path,
      metrics.ping_edition                                                                                                                AS ping_edition,
      subscription_info.total_arr                                                                                                         AS total_arr,
      subscription_info.total_subscription_count                                                                                          AS total_subscription_count,
      subscription_info.total_licensed_users                                                                                              AS total_licensed_users
    FROM subscription_info
        INNER JOIN metrics
    ON subscription_info.key = metrics.key
      AND subscription_info.ping_created_at_month = metrics.ping_created_at_month

)

 {{ dbt_audit(
     cte_ref="sub_combo",
     created_by="@icooper-acp",
     updated_by="@snalamaru",
     created_date="2022-04-07",
     updated_date="2022-06-08"
 ) }}
