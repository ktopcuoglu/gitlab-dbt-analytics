{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('mart_service_ping_instance_metric_28_day', 'mart_service_ping_instance_metric_28_day'),
    ('rpt_service_ping_instance_subcription_opt_in_monthly', 'rpt_service_ping_instance_subcription_opt_in_monthly'),
    ('mart_arr', 'mart_arr'),
    ('dim_service_ping_metric', 'dim_service_ping_metric')
    ])

}}

-- Assign key to subscription info (possible subscriptions)

, subscription_info AS (

    SELECT
    *,
    1                                         AS key
  FROM rpt_service_ping_instance_subcription_opt_in_monthly

-- Assign key to metric info (all metrics)

), metrics AS (

    SELECT
        *,
        1                                     AS key
    FROM dim_service_ping_metric

-- Join to get combo of all possible subscriptions and the metrics

), sub_combo AS (

    SELECT subscription_info.*,
            metrics_path                      AS metrics_path
    FROM subscription_info
        INNER JOIN metrics
    ON subscription_info.key = metrics.key

-- Get value from mart_arr

), arr_joined AS (

  SELECT
    mart_service_ping_instance_metric_28_day.*,
    mart_arr.quantity
  FROM mart_service_ping_instance_metric_28_day
    INNER JOIN mart_arr
  ON mart_service_ping_instance_metric_28_day.latest_active_subscription_id = mart_arr.dim_subscription_id
      AND mart_service_ping_instance_metric_28_day.ping_created_at_month = mart_arr.arr_month

-- Get actual count of subs/users for a given month/metric

), count_tbl AS (

    SELECT
        ping_created_at_month                           AS arr_month,
        metrics_path                                    AS metrics_path,
        stage_name                                      AS stage_name,
        section_name                                    AS section_name,
        group_name                                      AS group_name,
        is_smau                                         AS is_smau,
        is_gmau                                         AS is_gmau,
        is_paid_gmau                                    AS is_paid_gmau,
        is_umau                                         AS is_umau,
        COUNT(DISTINCT latest_active_subscription_id)   AS subscription_count,
        SUM(quantity)                                   AS seat_count
    FROM arr_joined
            WHERE latest_active_subscription_id IS NOT NULL
                AND is_last_ping_of_month = TRUE
                AND service_ping_delivery_type = 'Self-Managed'
                AND has_timed_out = FALSE
                AND metric_value is not null
    {{ dbt_utils.group_by(n=9)}}

-- Join actuals to number of possible subs/users

), joined_counts AS (

    SELECT
        count_tbl.arr_month                                     AS reporting_month,
        count_tbl.metrics_path                                  AS metrics_path,
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
        total_subscription_count - reported_subscription_count  AS no_reporting_subscription_count,
        total_licensed_users - reported_seat_count              AS no_reporting_seat_count
    FROM count_tbl
        LEFT JOIN sub_combo
    ON count_tbl.arr_month = sub_combo.arr_month
        AND count_tbl.metrics_path = sub_combo.metrics_path

-- Split subs and seats then union

), unioned_counts AS (

  SELECT
    reporting_month                                             AS reporting_month,
    metrics_path                                                AS metrics_path,
    stage_name                                                  AS stage_name,
    section_name                                                AS section_name,
    group_name                                                  AS group_name,
    is_smau                                                     AS is_smau,
    is_gmau                                                     AS is_gmau,
    is_paid_gmau                                                AS is_paid_gmau,
    is_umau                                                     AS is_umau,
    reported_subscription_count                                 AS reporting_count,
    no_reporting_subscription_count                             AS no_reporting_count,
    total_subscription_count                                    AS total_count,
    'reported metric - subscription based estimation'           AS estimation_grain
  FROM joined_counts

  UNION ALL

  SELECT
    reporting_month                                             AS reporting_month,
    metrics_path                                                AS metrics_path,
    stage_name                                                  AS stage_name,
    section_name                                                AS section_name,
    group_name                                                  AS group_name,
    is_smau                                                     AS is_smau,
    is_gmau                                                     AS is_gmau,
    is_paid_gmau                                                AS is_paid_gmau,
    is_umau                                                     AS is_umau,
    reported_seat_count                                         AS reporting_count,
    no_reporting_seat_count                                     AS no_reporting_count,
    total_licensed_users                                        AS total_count,
    'reported metric - seat based estimation'                   AS estimation_grain
  FROM joined_counts

-- Create PK and use macro for percent_reporting

), final AS (

SELECT
    {{ dbt_utils.surrogate_key(['reporting_month', 'metrics_path', 'estimation_grain']) }}          AS rpt_service_ping_instance_metric_adoption_subscription_monthly_id,
    *,
    {{ pct_w_counters('reporting_count', 'no_reporting_count') }}                                   AS percent_reporting
 FROM unioned_counts

)

 {{ dbt_audit(
     cte_ref="final",
     created_by="@icooper-acp",
     updated_by="@icooper-acp",
     created_date="2022-04-20",
     updated_date="2022-04-20"
 ) }}
