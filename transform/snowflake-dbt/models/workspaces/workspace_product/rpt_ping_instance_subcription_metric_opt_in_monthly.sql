{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('metric_opt_in', 'rpt_ping_counter_statistics'),
    ('active_subscriptions', 'rpt_ping_instance_active_subscriptions')
    ])

}}

/*
Determine latest version for each subscription to determine if the potential metric is valid for a given month
*/

, active_subscriptions_by_metric AS (

  SELECT
    active_subscriptions.*,
    metric_opt_in.metrics_path                                          AS metrics_path
  FROM active_subscriptions
    INNER JOIN metric_opt_in
      ON active_subscriptions.major_minor_version
        BETWEEN metric_opt_in.first_major_minor_version_with_counter AND metric_opt_in.last_major_minor_version_with_counter
        AND active_subscriptions.ping_edition = metric_opt_in.ping_edition

), agg_subscriptions AS (

SELECT
    {{ dbt_utils.surrogate_key(['ping_created_at_month', 'metrics_path']) }}          AS rpt_ping_instance_subcription_metric_opt_in_monthly_id,
    ping_created_at_month                                                             AS ping_created_at_month,
    metrics_path                                                                      AS metrics_path,
    ping_edition                                                                      AS ping_edition,
    SUM(arr)                                                                          AS total_arr,
    COUNT(DISTINCT latest_active_subscription_id)                                     AS total_subscription_count,
    SUM(licensed_user_count)                                                          AS total_licensed_users
FROM active_subscriptions_by_metric
    {{ dbt_utils.group_by(n=4)}}

)

 {{ dbt_audit(
     cte_ref="agg_subscriptions",
     created_by="@icooper-acp",
     updated_by="@icooper-acp",
     created_date="2022-04-20",
     updated_date="2022-04-20"
 ) }}
