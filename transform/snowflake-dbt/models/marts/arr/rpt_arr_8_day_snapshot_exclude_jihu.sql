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

)

{{ dbt_audit(
    cte_ref="arr_snapshot_plus_live_exclude_jihu",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-06-28",
    updated_date="2021-06-28"
) }}
