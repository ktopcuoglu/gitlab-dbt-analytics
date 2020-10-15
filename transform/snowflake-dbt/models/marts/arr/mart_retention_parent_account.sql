WITH dim_crm_accounts AS (

    SELECT *
    FROM {{ ref('dim_crm_accounts') }}

), dim_dates AS (

    SELECT *
    FROM {{ ref('dim_dates') }}

), dim_subscriptions AS (

    SELECT *
    FROM {{ ref('dim_subscriptions') }}

), fct_mrr AS (

    SELECT *
    FROM {{ ref('fct_mrr') }}

), parent_account_mrrs AS (

    SELECT
      merged_accounts.ultimate_parent_account_id,
      dim_dates.date_actual           AS mrr_month,
      dateadd('year', 1, date_actual) AS retention_month,
      SUM(mrr)                        AS mrr_total,
      SUM(arr)                        AS arr_total,
      SUM(quantity)                   AS quantity_total,
      MIN(subscription_end_month)     AS subscription_end_month
    FROM fct_mrr
    INNER JOIN dim_subscriptions
      ON dim_subscriptions.subscription_id = fct_mrr.subscription_id
    INNER JOIN dim_dates
      ON dim_dates.date_id = fct_mrr.date_id
    LEFT JOIN dim_crm_accounts AS crm_accounts
      ON crm_accounts.crm_account_id = fct_mrr.crm_account_id
    INNER JOIN dim_crm_accounts AS merged_accounts
      ON merged_accounts.crm_account_id = COALESCE(crm_accounts.merged_to_account_id, crm_accounts.crm_account_id)
    GROUP BY 1, 2, 3

), retention_subs AS (

    SELECT
      current_mrr.ultimate_parent_account_id,
      current_mrr.mrr_month          AS current_mrr_month,
      current_mrr.retention_month,
      current_mrr.mrr_total          AS current_mrr,
      future_mrr.mrr_total           AS future_mrr,
      current_mrr.arr_total          AS current_arr,
      future_mrr.arr_total           AS future_arr,
      current_mrr.quantity_total     AS current_quantity,
      future_mrr.quantity_total      AS future_quantity,
      current_mrr.subscription_end_month
    FROM parent_account_mrrs AS current_mrr
    LEFT JOIN parent_account_mrrs AS future_mrr
      ON current_mrr.ultimate_parent_account_id = future_mrr.ultimate_parent_account_id
        AND current_mrr.retention_month = future_mrr.mrr_month

), final AS (

    SELECT
      retention_subs.ultimate_parent_account_id AS parent_account_id,
      dim_crm_accounts.crm_account_name         AS parent_account_name,
      retention_month,
      dim_dates.fiscal_year                     AS retention_fiscal_year,
      dim_dates.fiscal_quarter                  AS retention_fiscal_quarter,
      subscription_end_month,
      current_mrr                               AS original_mrr,
      COALESCE(future_mrr, 0)                   AS net_retention_mrr,
      CASE WHEN net_retention_mrr > 0
        THEN least(net_retention_mrr, current_mrr)
        ELSE 0 END                              AS gross_retention_mrr,
      current_arr                               AS original_arr,
      COALESCE(future_arr, 0)                   AS retention_arr,
      current_quantity                          AS original_quantity,
      COALESCE(future_quantity, 0)              AS retention_quantity,
      {{ type_of_arr_change('retention_arr', 'original_arr') }},
      {{ reason_for_arr_change_seat_change('retention_quantity', 'original_quantity', 'retention_arr', 'original_arr') }},
      {{ reason_for_quantity_change_seat_change('retention_quantity', 'original_quantity') }},
      {{ reason_for_arr_change_price_change('product_category', 'previous_product_category', 'retention_quantity', 'original_quantity', 'retention_arr', 'original_arr', 'product_ranking',' previous_product_ranking') }},
      {{ annual_price_per_seat_change('retention_quantity', 'original_quantity', 'retention_arr', 'original_arr') }}
    FROM retention_subs
    INNER JOIN dim_dates
      ON dim_dates.date_actual = retention_subs.retention_month
    LEFT JOIN dim_crm_accounts
      ON dim_crm_accounts.crm_account_id = retention_subs.ultimate_parent_account_id
    WHERE retention_month <= dateadd(month, -1, CURRENT_DATE)

)

SELECT *
FROM final