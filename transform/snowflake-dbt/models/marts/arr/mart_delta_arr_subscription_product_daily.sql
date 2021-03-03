{{ simple_cte([
    ('dim_billing_account','dim_billing_account'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_date','dim_date'),
    ('dim_product_detail','dim_product_detail'),
    ('dim_subscription','dim_subscription'),
    ('fct_charge', 'fct_charge')
]) }}

, fct_charge_agg AS (

    SELECT
      dim_date.date_actual                                                                          AS arr_day,
      IFF(is_first_day_of_last_month_of_fiscal_quarter, fiscal_quarter_name_fy, NULL)               AS fiscal_quarter_name_fy,
      IFF(is_first_day_of_last_month_of_fiscal_year, fiscal_year, NULL)                             AS fiscal_year,
      unit_of_measure,
      dim_billing_account_id,
      dim_product_detail_id,
      dim_subscription_id,
      SUM(fct_charge.mrr)                                                                           AS mrr,
      SUM(fct_charge.quantity)                                                                      AS quantity
    FROM fct_charge
    INNER JOIN dim_date
      ON dim_date.date_id >= fct_charge.effective_start_date_id
      AND (dim_date.date_id < fct_charge.effective_end_date_id
        OR fct_charge.effective_end_date_id IS NULL)
    WHERE charge_type = 'Recurring'
    {{ dbt_utils.group_by(n=7) }}

), mart_arr AS (

    SELECT
      fct_charge_agg.arr_day,
      fiscal_quarter_name_fy,
      fiscal_year,
      dim_crm_account.parent_crm_account_name,
      dim_crm_account.dim_parent_crm_account_id,
      COALESCE(dim_crm_account.merged_to_account_id, dim_crm_account.dim_crm_account_id)            AS dim_crm_account_id,
      dim_subscription.subscription_name,
      dim_subscription.dim_subscription_id                                                          AS subscription_id,
      dim_product_detail.product_tier_name                                                          AS product_category,
      dim_product_detail.product_delivery_type                                                      AS delivery,
      dim_product_detail.product_ranking,
      mrr,
      quantity
    FROM fct_charge_agg
    INNER JOIN dim_subscription
      ON dim_subscription.dim_subscription_id = fct_charge_agg.dim_subscription_id
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_charge_agg.dim_product_detail_id
    INNER JOIN dim_billing_account
      ON dim_billing_account.dim_billing_account_id = fct_charge_agg.dim_billing_account_id
    LEFT JOIN dim_crm_account
      ON dim_billing_account.dim_crm_account_id = dim_crm_account.dim_crm_account_id

    WHERE dim_subscription.subscription_status NOT IN ('Draft', 'Expired')
      AND mrr != 0

), max_min_day AS (

    SELECT
      parent_crm_account_name,
      dim_parent_crm_account_id,
      dim_crm_account_id,
      subscription_name,
      subscription_id,
      product_category,
      delivery,
      product_ranking,
      MIN(arr_day)                        AS date_day_start,
      --add 1 month to generate churn month
      DATEADD('day',1,MAX(arr_day))       AS date_day_end
    FROM mart_arr
    {{ dbt_utils.group_by(n=8) }}

), base AS (

    SELECT
      parent_crm_account_name,
      dim_parent_crm_account_id,
      dim_crm_account_id,
      subscription_name,
      subscription_id,
      product_category,
      delivery,
      product_ranking,
      dim_date.date_actual AS arr_day,
      dim_date.fiscal_quarter_name_fy,
      dim_date.fiscal_year
    FROM max_min_day
    INNER JOIN dim_date
      -- all months after start date
      ON  dim_date.date_actual >= max_min_day.date_day_start
      -- up to and including end date
      AND dim_date.date_actual <=  max_min_day.date_day_end

), daily_arr_subscription_level AS (

    SELECT
      base.arr_day,
      base.parent_crm_account_name,
      base.dim_parent_crm_account_id,
      base.dim_crm_account_id,
      base.subscription_name,
      base.subscription_id,
      base.product_category,
      base.delivery,
      base.product_ranking,
      SUM(ZEROIFNULL(quantity))                                                               AS quantity,
      SUM(ZEROIFNULL(mrr)*12)                                                                 AS arr
    FROM base
    LEFT JOIN mart_arr
      ON base.arr_day = mart_arr.arr_day
      AND base.subscription_id = mart_arr.subscription_id
      AND base.product_category = mart_arr.product_category
    {{ dbt_utils.group_by(n=9) }}

), prior_day AS (

    SELECT
      daily_arr_subscription_level.*,
      COALESCE(LAG(quantity) OVER (PARTITION BY subscription_id, product_category ORDER BY arr_day),0) AS previous_quantity,
      COALESCE(LAG(arr) OVER (PARTITION BY subscription_id, product_category ORDER BY arr_day),0) AS previous_arr,
      ROW_NUMBER() OVER (PARTITION BY subscription_id, product_category ORDER BY arr_day) AS row_number
    FROM daily_arr_subscription_level

), type_of_arr_change AS (

    SELECT
      prior_day.*,
      {{ type_of_arr_change('arr','previous_arr','row_number') }}
    FROM prior_day

), reason_for_arr_change_beg AS (

    SELECT
      arr_day,
      subscription_id,
      product_category,
      previous_arr      AS beg_arr,
      previous_quantity AS beg_quantity
    FROM type_of_arr_change

), reason_for_arr_change_seat_change AS (

    SELECT
      arr_day,
      subscription_id,
      product_category,
      {{ reason_for_arr_change_seat_change('quantity', 'previous_quantity', 'arr', 'previous_arr') }},
      {{ reason_for_quantity_change_seat_change('quantity', 'previous_quantity') }}
    FROM type_of_arr_change

), reason_for_arr_change_price_change AS (

    SELECT
      arr_day,
      subscription_id,
      product_category,
      {{ reason_for_arr_change_price_change('product_category', 'product_category', 'quantity', 'previous_quantity', 'arr', 'previous_arr', 'product_ranking',' product_ranking') }}
    FROM type_of_arr_change

), reason_for_arr_change_end AS (

    SELECT
      arr_day,
      subscription_id,
      product_category,
      arr                   AS end_arr,
      quantity              AS end_quantity
    FROM type_of_arr_change

), annual_price_per_seat_change AS (

    SELECT
      arr_day,
      subscription_id,
      product_category,
      {{ annual_price_per_seat_change('quantity', 'previous_quantity', 'arr', 'previous_arr') }}
    FROM type_of_arr_change

), combined AS (

    SELECT
      {{ dbt_utils.surrogate_key(['type_of_arr_change.arr_day', 'type_of_arr_change.subscription_id','type_of_arr_change.product_category']) }}
                                                                    AS primary_key,
      type_of_arr_change.arr_day,
      type_of_arr_change.parent_crm_account_name,
      type_of_arr_change.dim_parent_crm_account_id,
      type_of_arr_change.dim_crm_account_id,
      type_of_arr_change.subscription_name,
      type_of_arr_change.subscription_id,
      type_of_arr_change.product_category,
      type_of_arr_change.delivery,
      type_of_arr_change.product_ranking,
      type_of_arr_change.type_of_arr_change,
      reason_for_arr_change_beg.beg_arr,
      reason_for_arr_change_beg.beg_quantity,
      reason_for_arr_change_seat_change.seat_change_arr,
      reason_for_arr_change_seat_change.seat_change_quantity,
      reason_for_arr_change_price_change.price_change_arr,
      reason_for_arr_change_end.end_arr,
      reason_for_arr_change_end.end_quantity,
      annual_price_per_seat_change.annual_price_per_seat_change
    FROM type_of_arr_change
    LEFT JOIN reason_for_arr_change_beg
      ON type_of_arr_change.subscription_id = reason_for_arr_change_beg.subscription_id
      AND type_of_arr_change.arr_day = reason_for_arr_change_beg.arr_day
      AND type_of_arr_change.product_category = reason_for_arr_change_beg.product_category
    LEFT JOIN reason_for_arr_change_seat_change
      ON type_of_arr_change.subscription_id = reason_for_arr_change_seat_change.subscription_id
      AND type_of_arr_change.arr_day = reason_for_arr_change_seat_change.arr_day
      AND type_of_arr_change.product_category = reason_for_arr_change_seat_change.product_category
    LEFT JOIN reason_for_arr_change_price_change
      ON type_of_arr_change.subscription_id = reason_for_arr_change_price_change.subscription_id
      AND type_of_arr_change.arr_day = reason_for_arr_change_price_change.arr_day
      AND type_of_arr_change.product_category = reason_for_arr_change_price_change.product_category
    LEFT JOIN reason_for_arr_change_end
      ON type_of_arr_change.subscription_id = reason_for_arr_change_end.subscription_id
      AND type_of_arr_change.arr_day = reason_for_arr_change_end.arr_day
      AND type_of_arr_change.product_category = reason_for_arr_change_end.product_category
    LEFT JOIN annual_price_per_seat_change
      ON type_of_arr_change.subscription_id = annual_price_per_seat_change.subscription_id
      AND type_of_arr_change.arr_day = annual_price_per_seat_change.arr_day
      AND type_of_arr_change.product_category = annual_price_per_seat_change.product_category

)

SELECT *
FROM combined
