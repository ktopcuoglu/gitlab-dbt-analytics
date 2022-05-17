{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('mart_ping_instance_metric_monthly', 'mart_ping_instance_metric_monthly'),
    ('potential_report_counts', 'rpt_ping_instance_subscription_metric_opt_in_monthly'),
    ('active_subscriptions', 'rpt_ping_instance_active_subscriptions'),
    ('dim_ping_metric', 'dim_ping_metric')
    ])

}}

-- Get value from active_susbcriptions

, arr_joined AS (

  SELECT
    mart_ping_instance_metric_monthly.ping_created_at_month                           AS ping_created_at_month,
    mart_ping_instance_metric_monthly.metrics_path                                    AS metrics_path,
    mart_ping_instance_metric_monthly.ping_edition                                    AS ping_edition,
    mart_ping_instance_metric_monthly.stage_name                                      AS stage_name,
    mart_ping_instance_metric_monthly.section_name                                    AS section_name,
    mart_ping_instance_metric_monthly.group_name                                      AS group_name,
    mart_ping_instance_metric_monthly.is_smau                                         AS is_smau,
    mart_ping_instance_metric_monthly.is_gmau                                         AS is_gmau,
    mart_ping_instance_metric_monthly.is_paid_gmau                                    AS is_paid_gmau,
    mart_ping_instance_metric_monthly.is_umau                                         AS is_umau,
    mart_ping_instance_metric_monthly.latest_active_subscription_id                   AS latest_active_subscription_id,
    active_subscriptions.licensed_user_count                                          AS licensed_user_count
  FROM mart_ping_instance_metric_monthly
    INNER JOIN active_subscriptions
  ON mart_ping_instance_metric_monthly.latest_active_subscription_id = active_subscriptions.latest_active_subscription_id
      AND mart_ping_instance_metric_monthly.ping_created_at_month = active_subscriptions.ping_created_at_month
    WHERE time_frame = '28d'
      AND ping_delivery_type = 'Self-Managed'
    {{ dbt_utils.group_by(n=12)}}

-- Get actual count of subs/users for a given month/metric

), reported_actuals AS (

    SELECT
        ping_created_at_month                                         AS ping_created_at_month,
        metrics_path                                                  AS metrics_path,
        ping_edition                                                  AS ping_edition,
        stage_name                                                    AS stage_name,
        section_name                                                  AS section_name,
        group_name                                                    AS group_name,
        is_smau                                                       AS is_smau,
        is_gmau                                                       AS is_gmau,
        is_paid_gmau                                                  AS is_paid_gmau,
        is_umau                                                       AS is_umau,
        COUNT(DISTINCT latest_active_subscription_id)                 AS subscription_count,
        SUM(licensed_user_count)                                      AS seat_count
    FROM arr_joined
    {{ dbt_utils.group_by(n=10)}}

-- Join actuals to number of possible subs/users

), joined_counts AS (

    SELECT
        reported_actuals.ping_created_at_month                         AS ping_created_at_month,
        reported_actuals.metrics_path                                  AS metrics_path,
        reported_actuals.ping_edition                                  AS ping_edition,
        reported_actuals.stage_name                                    AS stage_name,
        reported_actuals.section_name                                  AS section_name,
        reported_actuals.group_name                                    AS group_name,
        reported_actuals.is_smau                                       AS is_smau,
        reported_actuals.is_gmau                                       AS is_gmau,
        reported_actuals.is_paid_gmau                                  AS is_paid_gmau,
        reported_actuals.is_umau                                       AS is_umau,
        reported_actuals.subscription_count                            AS reported_subscription_count, -- actually reported
        reported_actuals.seat_count                                    AS reported_seat_count, -- actually reported
        potential_report_counts.total_licensed_users                   AS total_licensed_users,  -- could have reported
        potential_report_counts.total_subscription_count               AS total_subscription_count, -- could have reported
        total_subscription_count - reported_subscription_count         AS not_reporting_subscription_count, -- could have reported, but didn't
        total_licensed_users - reported_seat_count                     AS not_reporting_seat_count -- could have reported, but didn't
    FROM reported_actuals
        INNER JOIN potential_report_counts
    ON reported_actuals.ping_created_at_month = potential_report_counts.ping_created_at_month
        AND reported_actuals.metrics_path = potential_report_counts.metrics_path
        AND reported_actuals.ping_edition = potential_report_counts.ping_edition

-- Split subs and seats then union

), unioned_counts AS (

  SELECT
    ping_created_at_month                                           AS ping_created_at_month,
    metrics_path                                                    AS metrics_path,
    ping_edition                                                    AS ping_edition,
    stage_name                                                      AS stage_name,
    section_name                                                    AS section_name,
    group_name                                                      AS group_name,
    is_smau                                                         AS is_smau,
    is_gmau                                                         AS is_gmau,
    is_paid_gmau                                                    AS is_paid_gmau,
    is_umau                                                         AS is_umau,
    reported_subscription_count                                     AS reporting_count,
    not_reporting_subscription_count                                 AS not_reporting_count,
    total_subscription_count                                        AS total_count,
    'metric/version check - subscription based estimation'          AS estimation_grain
  FROM joined_counts

  UNION ALL

  SELECT
    ping_created_at_month                                           AS ping_created_at_month,
    metrics_path                                                    AS metrics_path,
    ping_edition                                                    AS ping_edition,
    stage_name                                                      AS stage_name,
    section_name                                                    AS section_name,
    group_name                                                      AS group_name,
    is_smau                                                         AS is_smau,
    is_gmau                                                         AS is_gmau,
    is_paid_gmau                                                    AS is_paid_gmau,
    is_umau                                                         AS is_umau,
    reported_seat_count                                             AS reporting_count,
    not_reporting_seat_count                                         AS not_reporting_count,
    total_licensed_users                                            AS total_count,
    'metric/version check - seat based estimation'                  AS estimation_grain
  FROM joined_counts

-- Create PK and use macro for percent_reporting

), final AS (

SELECT
    {{ dbt_utils.surrogate_key(['ping_created_at_month', 'metrics_path', 'ping_edition','estimation_grain']) }}          AS rpt_ping_instance_metric_adoption_subscription_metric_monthly_id,
    *,
    {{ pct_w_counters('reporting_count', 'not_reporting_count') }}                                                        AS percent_reporting
 FROM unioned_counts

)

 {{ dbt_audit(
     cte_ref="final",
     created_by="@icooper-acp",
     updated_by="@icooper-acp",
     created_date="2022-04-07",
     updated_date="2022-04-15"
 ) }}
