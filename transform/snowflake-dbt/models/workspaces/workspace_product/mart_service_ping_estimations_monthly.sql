{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('fct_charge', 'fct_charge'),
    ('dim_charge', 'dim_charge'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_date', 'dim_date'),
    ('dim_product_detail', 'dim_product_detail'),
    ('dim_usage_ping_metric', 'dim_usage_ping_metric'),
    ('dim_product_detail', 'dim_product_detail'),
    ('mart_service_ping_instance_metric_28_day', 'mart_service_ping_instance_metric_28_day'),
    ('dim_license', 'dim_license'),
    ('dim_hosts', 'dim_hosts'),
    ('dim_location', 'dim_location_country'),
    ('dim_usage_ping_metric', 'dim_usage_ping_metric')
    ])

}}

, subscription_info AS (
    SELECT
      dim_date.date_actual                    AS arr_month,
      fct_charge.dim_subscription_id          AS dim_subscription_id,
      dim_subscription.subscription_status    AS subscription_status,
      SUM(fct_charge.mrr * 12)                AS arr,
      SUM(fct_charge.quantity)                AS licensed_users,
      1                                       AS key
    FROM fct_charge
    LEFT JOIN dim_charge
      ON fct_charge.dim_charge_id = dim_charge.dim_charge_id
    LEFT JOIN dim_subscription
      ON fct_charge.dim_subscription_id = dim_subscription.dim_subscription_id
    INNER JOIN dim_date
      ON fct_charge.effective_start_month <= dim_date.date_actual
      AND (fct_charge.effective_end_month > dim_date.date_actual
           OR fct_charge.effective_end_month IS NULL)
      AND dim_date.day_of_month = 1
    LEFT JOIN dim_product_detail
      ON fct_charge.dim_product_detail_id = dim_product_detail.dim_product_detail_id
    WHERE
    --dim_charge.is_included_in_arr_calc = TRUE
      dim_date.date_actual <= DATE_TRUNC('month',CURRENT_DATE)
      AND product_tier_name != 'Storage'
      AND product_delivery_type = 'Self-Managed'
    GROUP BY 1,2,3
    ORDER BY 1 DESC, 2 --68.8

), metrics AS (

    SELECT *,
        1 AS key
    FROM dim_usage_ping_metric

), sub_combo AS (

    SELECT subscription_info.*,
            metrics_path
    FROM subscription_info
        INNER JOIN metrics
    ON subscription_info.key = metrics.key


), fact AS (

    SELECT
        metrics_path,
        metric_value,
        latest_active_subscription_id,
        dim_date_id,
        ping_edition,
        stage_name,
        section_name,
        group_name,
        is_smau,
        is_gmau,
        is_paid_gmau,
        is_umau
    FROM mart_service_ping_instance_metric_28_day
        WHERE latest_active_subscription_id IS NOT NULL
            AND is_last_ping_of_month = TRUE
            AND service_ping_delivery_type = 'Self-Managed'
            AND has_timed_out = FALSE


), fact_w_month AS (

    SELECT metrics_path,
        dim_date.first_day_of_month,
        latest_active_subscription_id,
        ping_edition,
        stage_name,
        section_name,
        group_name,
        is_smau,
        is_gmau,
        is_paid_gmau,
        is_umau,
        SUM(metric_value)               AS metric_value
    FROM fact
        INNER JOIN dim_date
            ON fact.dim_date_id = dim_date.date_id
    {{ dbt_utils.group_by(n=11)}}

), subs_w_fct AS (

    SELECT
        sub_combo.*,
        fact_w_month.metric_value,
        fact_w_month.ping_edition,
        fact_w_month.stage_name,
        fact_w_month.section_name,
        fact_w_month.group_name,
        fact_w_month.is_smau,
        fact_w_month.is_gmau,
        fact_w_month.is_paid_gmau,
        fact_w_month.is_umau
    FROM sub_combo
        LEFT JOIN fact_w_month
    ON sub_combo.arr_month = fact_w_month.first_day_of_month
        AND sub_combo.dim_subscription_id = fact_w_month.latest_active_subscription_id
        AND sub_combo.metrics_path = fact_w_month.metrics_path

), count_tbl AS (

    SELECT
        arr_month,
        metrics_path,ping_edition,
        stage_name,
        section_name,
        group_name,
        is_smau,
        is_gmau,
        is_paid_gmau,
        is_umau,
        COUNT(dim_subscription_id)              AS subscription_count,
        SUM(licensed_users)                     AS seat_count,
        SUM(metric_value)                       AS metric_count
    FROM subs_w_fct
        WHERE metric_value is not null
    {{ dbt_utils.group_by(n=10)}}

), subs_wo_counts AS (

    SELECT
        arr_month,
        metrics_path,
        COUNT(dim_subscription_id)              AS subscription_count,
        SUM(licensed_users)                     AS seat_count,
        0                                       AS metric_count
    FROM subs_w_fct
        WHERE metric_value is null
    {{ dbt_utils.group_by(n=2)}}

), joined_counts AS (

    SELECT
        count_tbl.arr_month                  AS month,
        count_tbl.metrics_path               AS metrics_path,
        count_tbl.ping_edition               AS ping_edition,
        count_tbl.stage_name                 AS stage_name,
        count_tbl.section_name               AS section_name,
        count_tbl.group_name                 AS group_name,
        count_tbl.is_smau                    AS is_smau,
        count_tbl.is_gmau                    AS is_gmau,
        count_tbl.is_paid_gmau               AS is_paid_gmau,
        count_tbl.is_umau                    AS is_umau,
        count_tbl.subscription_count         AS active_subscription_count,
        count_tbl.seat_count                 AS active_seat_count,
        count_tbl.metric_count               AS active_metric_count,
        subs_wo_counts.subscription_count    AS inactive_subscription_count,
        subs_wo_counts.seat_count            AS inactive_seat_count
    FROM count_tbl
        LEFT JOIN subs_wo_counts
    ON count_tbl.arr_month = subs_wo_counts.arr_month
        AND count_tbl.metrics_path = subs_wo_counts.metrics_path

), final AS (

SELECT
    {{ dbt_utils.surrogate_key(['month', 'metrics_path']) }}                                                                          AS mart_service_ping_estimations_monthly_id,
    *,
    {{ pct_w_counters('active_subscription_count', 'inactive_subscription_count') }}                                                  AS pct_subscriptions_with_counters,
    {{ pct_w_counters('active_seat_count', 'inactive_seat_count') }}                                                                  AS pct_users_with_counters,
    {{ usage_estimation('active_metric_count', 'pct_subscriptions_with_counters') }}                                                  AS estimated_sub_based_usage,
    {{ usage_estimation('active_metric_count', 'pct_users_with_counters') }}                                                          AS estimated_user_based_usage
 FROM joined_counts

)

 {{ dbt_audit(
     cte_ref="final",
     created_by="@icooper-acp",
     updated_by="@icooper-acp",
     created_date="2022-04-07",
     updated_date="2022-04-07"
 ) }}

 /*
Union seat/subscription v join and add flag to make it easy for reporting
 */
