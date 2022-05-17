{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('mart_ping_instance_metric_monthly', 'mart_ping_instance_metric_monthly'),
    ('sub_combo', 'rpt_ping_instance_subscription_opt_in_monthly'),
    ('active_subscriptions', 'rpt_ping_instance_active_subscriptions')
    ])

}}

-- Assign key to subscription info (possible subscriptions)

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

), count_tbl AS (

    SELECT
        ping_created_at_month                           AS ping_created_at_month,
        metrics_path                                    AS metrics_path,
        ping_edition                                    AS ping_edition,
        stage_name                                      AS stage_name,
        section_name                                    AS section_name,
        group_name                                      AS group_name,
        is_smau                                         AS is_smau,
        is_gmau                                         AS is_gmau,
        is_paid_gmau                                    AS is_paid_gmau,
        is_umau                                         AS is_umau,
        COUNT(DISTINCT latest_active_subscription_id)   AS subscription_count,
        SUM(licensed_user_count)                        AS seat_count
    FROM arr_joined
    {{ dbt_utils.group_by(n=10)}}

-- Join actuals to number of possible subs/users

), joined_counts AS (

    SELECT
        count_tbl.ping_created_at_month                         AS ping_created_at_month,
        count_tbl.metrics_path                                  AS metrics_path,
        count_tbl.ping_edition                                  AS ping_edition,
        count_tbl.stage_name                                    AS stage_name,
        count_tbl.section_name                                  AS section_name,
        count_tbl.group_name                                    AS group_name,
        count_tbl.is_smau                                       AS is_smau,
        count_tbl.is_gmau                                       AS is_gmau,
        count_tbl.is_paid_gmau                                  AS is_paid_gmau,
        count_tbl.is_umau                                       AS is_umau,
        count_tbl.subscription_count                            AS reported_subscription_count,
        count_tbl.seat_count                                    AS reported_seat_count,
        sub_combo.total_licensed_users                          AS total_licensed_users,
        sub_combo.total_subscription_count                      AS total_subscription_count,
        total_subscription_count - reported_subscription_count  AS not_reporting_subscription_count,
        total_licensed_users - reported_seat_count              AS not_reporting_seat_count
    FROM count_tbl
        LEFT JOIN sub_combo
    ON count_tbl.ping_created_at_month = sub_combo.ping_created_at_month
        AND count_tbl.metrics_path = sub_combo.metrics_path
        AND count_tbl.ping_edition = sub_combo.ping_edition

-- Split subs and seats then union

), unioned_counts AS (

  SELECT
    ping_created_at_month                                       AS ping_created_at_month,
    metrics_path                                                AS metrics_path,
    ping_edition                                                AS ping_edition,
    stage_name                                                  AS stage_name,
    section_name                                                AS section_name,
    group_name                                                  AS group_name,
    is_smau                                                     AS is_smau,
    is_gmau                                                     AS is_gmau,
    is_paid_gmau                                                AS is_paid_gmau,
    is_umau                                                     AS is_umau,
    reported_subscription_count                                 AS reporting_count,
    not_reporting_subscription_count                             AS not_reporting_count,
    total_subscription_count                                    AS total_count,
    'reported metric - subscription based estimation'           AS estimation_grain
  FROM joined_counts

  UNION ALL

  SELECT
    ping_created_at_month                                       AS ping_created_at_month,
    metrics_path                                                AS metrics_path,
    ping_edition                                                AS ping_edition,
    stage_name                                                  AS stage_name,
    section_name                                                AS section_name,
    group_name                                                  AS group_name,
    is_smau                                                     AS is_smau,
    is_gmau                                                     AS is_gmau,
    is_paid_gmau                                                AS is_paid_gmau,
    is_umau                                                     AS is_umau,
    reported_seat_count                                         AS reporting_count,
    not_reporting_seat_count                                     AS not_reporting_count,
    total_licensed_users                                        AS total_count,
    'reported metric - seat based estimation'                   AS estimation_grain
  FROM joined_counts

-- Create PK and use macro for percent_reporting

), final AS (

SELECT
    {{ dbt_utils.surrogate_key(['ping_created_at_month', 'metrics_path', 'ping_edition', 'estimation_grain']) }}          AS rpt_ping_instance_metric_adoption_subscription_monthly_id,
    *,
    {{ pct_w_counters('reporting_count', 'not_reporting_count') }}                                                         AS percent_reporting
 FROM unioned_counts

)

 {{ dbt_audit(
     cte_ref="final",
     created_by="@icooper-acp",
     updated_by="@icooper-acp",
     created_date="2022-04-20",
     updated_date="2022-04-20"
 ) }}
