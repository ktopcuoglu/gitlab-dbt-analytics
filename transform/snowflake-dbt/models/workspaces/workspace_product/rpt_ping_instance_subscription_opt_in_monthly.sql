{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
  ('active_subscriptions', 'rpt_ping_instance_active_subscriptions'),
  ('dim_ping_metric', 'dim_ping_metric')
    ])

}}

-- Determine monthly sub and user count

, dedupe_active_subscriptions AS (

  SELECT
    ping_created_at_month                                             AS ping_created_at_month,
    ping_edition                                                      AS ping_edition,
    latest_active_subscription_id                                     AS latest_active_subscription_id,
    licensed_user_count                                               AS licensed_user_count,
    arr                                                               AS arr
  FROM active_subscriptions
    {{ dbt_utils.group_by(n=5)}}

), subscription_info AS (

  SELECT
    ping_created_at_month                                             AS ping_created_at_month,
    ping_edition                                                      AS ping_edition,
    1                                                                 AS key,
    SUM(arr)                                                          AS total_arr,
    COUNT(DISTINCT latest_active_subscription_id)                     AS total_subscription_count,
    SUM(licensed_user_count)                                          AS total_licensed_users
  FROM dedupe_active_subscriptions
      {{ dbt_utils.group_by(n=3)}}

), metrics AS (

    SELECT
        *,
        1                                     AS key
    FROM dim_ping_metric

-- Join to get combo of all possible subscriptions and the metrics

), sub_combo AS (

    SELECT
      {{ dbt_utils.surrogate_key(['ping_created_at_month', 'metrics_path', 'ping_edition']) }}          AS rpt_ping_instance_subscription_opt_in_monthly_id,
      subscription_info.ping_created_at_month                                                           AS ping_created_at_month,
      metrics.metrics_path                                                                              AS metrics_path,
      subscription_info.ping_edition                                                                    AS ping_edition,
      subscription_info.total_arr                                                                       AS total_arr,
      subscription_info.total_subscription_count                                                        AS total_subscription_count,
      subscription_info.total_licensed_users                                                            AS total_licensed_users
    FROM subscription_info
        INNER JOIN metrics
    ON subscription_info.key = metrics.key

)

 {{ dbt_audit(
     cte_ref="sub_combo",
     created_by="@icooper-acp",
     updated_by="@icooper-acp",
     created_date="2022-04-07",
     updated_date="2022-04-15"
 ) }}
