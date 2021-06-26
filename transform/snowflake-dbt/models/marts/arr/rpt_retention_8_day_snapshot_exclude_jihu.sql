{{config({
    "schema": "common_mart_sales"
  })
}}

{{ simple_cte([
    ('mart_arr_snapshot_bottom_up','mart_arr_snapshot_bottom_up'),
    ('mart_arr','mart_arr'),
    ('dim_date','dim_date')
]) }}

, snapshot_dates AS (
  --Use the 8th calendar day to snapshot ARR, Licensed Users, and Customer Count Metrics

    SELECT DISTINCT
      first_day_of_month,
      DATEADD('day',7,DATEADD('month',1,first_day_of_month)) AS snapshot_date_fpa
    FROM dim_date
    ORDER BY 1 DESC

), arr_snapshot_plus_live_exclude_jihu AS (

    SELECT
      mart_arr_snapshot_bottom_up.arr_month,
      mart_arr_snapshot_bottom_up.fiscal_quarter_name_fy,
      mart_arr_snapshot_bottom_up.fiscal_year,
      mart_arr_snapshot_bottom_up.subscription_start_month,
      mart_arr_snapshot_bottom_up.subscription_end_month,
      mart_arr_snapshot_bottom_up.dim_billing_account_id,
      mart_arr_snapshot_bottom_up.sold_to_country,
      mart_arr_snapshot_bottom_up.billing_account_name,
      mart_arr_snapshot_bottom_up.billing_account_number,
      mart_arr_snapshot_bottom_up.dim_crm_account_id,
      mart_arr_snapshot_bottom_up.dim_parent_crm_account_id,
      mart_arr_snapshot_bottom_up.parent_crm_account_name,
      mart_arr_snapshot_bottom_up.parent_crm_account_billing_country,
      mart_arr_snapshot_bottom_up.parent_crm_account_sales_segment,
      mart_arr_snapshot_bottom_up.parent_crm_account_industry,
      mart_arr_snapshot_bottom_up.parent_crm_account_owner_team,
      mart_arr_snapshot_bottom_up.parent_crm_account_sales_territory,
      mart_arr_snapshot_bottom_up.subscription_name,
      mart_arr_snapshot_bottom_up.subscription_status,
      mart_arr_snapshot_bottom_up.subscription_sales_type,
      mart_arr_snapshot_bottom_up.product_tier_name,
      mart_arr_snapshot_bottom_up.product_delivery_type,
      mart_arr_snapshot_bottom_up.service_type,
      mart_arr_snapshot_bottom_up.unit_of_measure,
      mart_arr_snapshot_bottom_up.mrr,
      mart_arr_snapshot_bottom_up.arr,
      mart_arr_snapshot_bottom_up.quantity,
      mart_arr_snapshot_bottom_up.arr_band_calc,
      mart_arr_snapshot_bottom_up.parent_account_cohort_month,
      mart_arr_snapshot_bottom_up.months_since_parent_account_cohort_start,
      mart_arr_snapshot_bottom_up.parent_crm_account_employee_count_band
    FROM mart_arr_snapshot_bottom_up
    INNER JOIN snapshot_dates
      ON mart_arr_snapshot_bottom_up.arr_month = snapshot_dates.first_day_of_month
      AND mart_arr_snapshot_bottom_up.snapshot_date = snapshot_dates.snapshot_date_fpa
    WHERE is_jihu_account = FALSE

    UNION ALL

    SELECT
      mart_arr.arr_month,
      mart_arr.fiscal_quarter_name_fy,
      mart_arr.fiscal_year,
      mart_arr.subscription_start_month,
      mart_arr.subscription_end_month,
      mart_arr.dim_billing_account_id,
      mart_arr.sold_to_country,
      mart_arr.billing_account_name,
      mart_arr.billing_account_number,
      mart_arr.dim_crm_account_id,
      mart_arr.dim_parent_crm_account_id,
      mart_arr.parent_crm_account_name,
      mart_arr.parent_crm_account_billing_country,
      mart_arr.parent_crm_account_sales_segment,
      mart_arr.parent_crm_account_industry,
      mart_arr.parent_crm_account_owner_team,
      mart_arr.parent_crm_account_sales_territory,
      mart_arr.subscription_name,
      mart_arr.subscription_status,
      mart_arr.subscription_sales_type,
      mart_arr.product_tier_name,
      mart_arr.product_delivery_type,
      mart_arr.service_type,
      mart_arr.unit_of_measure,
      mart_arr.mrr,
      mart_arr.arr,
      mart_arr.quantity,
      mart_arr.arr_band_calc,
      mart_arr.parent_account_cohort_month,
      mart_arr.months_since_parent_account_cohort_start,
      mart_arr.parent_crm_account_employee_count_band
    FROM mart_arr
    WHERE arr_month <= '2020-01-01'
      AND is_jihu_account = FALSE
    ORDER BY arr_month DESC, dim_parent_crm_account_id, dim_crm_account_id

), parent_account_mrrs AS (

    SELECT
      dim_parent_crm_account_id,
      parent_crm_account_name,
      arr_month                                         AS mrr_month,
      dateadd('year', 1, arr_month)                     AS retention_month,
      arr_band_calc,
      parent_crm_account_employee_count_band,
      SUM(ZEROIFNULL(mrr))                              AS mrr_total,
      SUM(ZEROIFNULL(arr))                              AS arr_total,
      SUM(ZEROIFNULL(quantity))                         AS quantity_total
    FROM arr_snapshot_plus_live_exclude_jihu
    {{ dbt_utils.group_by(n=6) }}

), retention_subs AS (

    SELECT
      current_mrr.dim_parent_crm_account_id,
      current_mrr.parent_crm_account_name,
      current_mrr.mrr_month                             AS current_mrr_month,
      current_mrr.retention_month,
      current_mrr.mrr_total                             AS current_mrr,
      future_mrr.mrr_total                              AS future_mrr,
      current_mrr.arr_total                             AS current_arr,
      future_mrr.arr_total                              AS future_arr,
      current_mrr.quantity_total                        AS current_quantity,
      future_mrr.quantity_total                         AS future_quantity,
      future_mrr.parent_crm_account_employee_count_band AS parent_crm_account_employee_count_band
    FROM parent_account_mrrs AS current_mrr
    LEFT JOIN parent_account_mrrs AS future_mrr
      ON current_mrr.dim_parent_crm_account_id = future_mrr.dim_parent_crm_account_id
      AND current_mrr.retention_month = future_mrr.mrr_month

), retention_logic AS (

    SELECT
      retention_subs.dim_parent_crm_account_id,
      retention_subs.parent_crm_account_name,
      retention_month,
      IFF(is_first_day_of_last_month_of_fiscal_year, fiscal_year, NULL)
                                                AS retention_year,
      IFF(is_first_day_of_last_month_of_fiscal_quarter, fiscal_quarter_name_fy, NULL)
                                                AS retention_quarter,
      current_mrr                               AS prior_year_mrr,
      COALESCE(future_mrr, 0)                   AS net_retention_mrr,
      CASE WHEN net_retention_mrr > 0
        THEN least(net_retention_mrr, current_mrr)
        ELSE 0 END                              AS gross_retention_mrr,
      current_arr                               AS prior_year_arr,
      COALESCE(future_arr, 0)                   AS net_retention_arr,
      CASE WHEN net_retention_arr > 0
        THEN least(net_retention_arr, current_arr)
        ELSE 0 END                              AS gross_retention_arr,
      current_quantity                          AS prior_year_quantity,
      COALESCE(future_quantity, 0)              AS net_retention_quantity,
      CASE
        WHEN COALESCE(future_arr, 0) > 5000 THEN 'ARR > $5K'
        WHEN COALESCE(future_arr, 0) <= 5000 THEN 'ARR <= $5K'
      END                                       AS retention_arr_band,
      parent_crm_account_employee_count_band
    FROM retention_subs
    INNER JOIN dim_date
      ON dim_date.date_actual = retention_subs.retention_month
    WHERE retention_month < DATE_TRUNC('month', CURRENT_DATE)

), final_table AS (

    SELECT
      dim_parent_crm_account_id,
      parent_crm_account_name,
      retention_year,
      retention_quarter,
      retention_month,
      retention_arr_band,
      parent_crm_account_employee_count_band,
      SUM(prior_year_arr)                           AS prior_year_arr,
      SUM(net_retention_arr)                        AS net_retention_arr,
      SUM(gross_retention_arr)                      AS gross_retention_arr
    FROM retention_logic
    {{ dbt_utils.group_by(n=7) }}

)

{{ dbt_audit(
    cte_ref="final_table",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-06-28",
    updated_date="2021-06-28"
) }}
