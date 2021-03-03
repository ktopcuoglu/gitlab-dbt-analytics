WITH dim_date AS (

    SELECT *
    FROM {{ ref('dim_date') }}

), dim_billing_account AS (

    SELECT *
    FROM {{ ref('dim_billing_account') }}

), dim_crm_account AS (

    SELECT *
    FROM {{ ref('dim_crm_account') }}

), dim_product_detail AS (

    SELECT *
    FROM {{ ref('dim_product_detail') }}

), fct_invoice_item AS (

    SELECT *
    FROM {{ ref('fct_invoice_item') }}
    WHERE is_last_segment_version = TRUE
      AND arr != 0

), zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), arr_agg AS (

    SELECT
      fct_invoice_item.effective_start_month,
      fct_invoice_item.effective_end_month,
      dim_billing_account_id_subscription,
      dim_crm_account_id_subscription,
      dim_billing_account_id_invoice,
      dim_crm_account_id_invoice,
      dim_subscription_id,
      dim_product_detail_id,
      SUM(invoice_item_charge_amount)                      AS invoice_item_charge_amount,
      SUM(mrr)                                             AS mrr,
      SUM(arr)                                             AS arr,
      SUM(quantity)                                        AS quantity
    FROM fct_invoice_item
    WHERE fct_invoice_item.effective_end_month > fct_invoice_item.effective_start_month OR fct_invoice_item.effective_end_month IS NULL
      --filter out 2 subscription_ids with known data quality issues when comparing invoiced subscriptions to the Zuora UI.
      AND dim_subscription_id NOT IN ('2c92a0ff5e1dcf14015e3c191d4f7689','2c92a00e6a3477b5016a46aaec2f08bc')
    {{ dbt_utils.group_by(n=8) }}

), combined AS (

    SELECT
      {{ dbt_utils.surrogate_key(['dim_crm_account_invoice.dim_parent_crm_account_id', 'arr_agg.effective_start_month', 'arr_agg.effective_end_month', 'zuora_subscription.subscription_name', 'arr_agg.dim_product_detail_id']) }}
                                                                        AS primary_key,
      arr_agg.effective_start_month,
      arr_agg.effective_end_month,
      DATE_TRUNC('month',zuora_subscription.subscription_start_date)    AS subscription_start_month,
      DATE_TRUNC('month',zuora_subscription.subscription_end_date)      AS subscription_end_month,
      dim_crm_account_invoice.dim_parent_crm_account_id                 AS dim_parent_crm_account_id_invoice,
      dim_crm_account_invoice.parent_crm_account_name                   AS parent_crm_account_name_invoice,
      dim_crm_account_invoice.parent_crm_account_billing_country        AS parent_crm_account_billing_country_invoice,
      dim_crm_account_invoice.parent_crm_account_sales_segment          AS parent_crm_account_sales_segment_invoice,
      dim_crm_account_invoice.dim_crm_account_id                        AS dim_crm_account_id_invoice,
      dim_crm_account_invoice.crm_account_name                          AS crm_account_name_invoice,
      dim_crm_account_invoice.crm_account_owner_team                    AS crm_account_owner_team_invoice,
      dim_crm_account_subscription.dim_parent_crm_account_id            AS dim_parent_crm_account_id_subscription,
      dim_crm_account_subscription.parent_crm_account_name              AS parent_crm_account_name_subscription,
      dim_crm_account_subscription.parent_crm_account_billing_country   AS parent_crm_account_billing_country_subscription,
      dim_crm_account_subscription.parent_crm_account_sales_segment     AS parent_crm_account_sales_segment_subscription,
      dim_crm_account_subscription.dim_crm_account_id                   AS dim_crm_account_id_subscription,
      dim_crm_account_subscription.crm_account_name                     AS crm_account_name_subscription,
      dim_crm_account_subscription.crm_account_owner_team               AS crm_account_owner_team_subscription,
      zuora_subscription.subscription_name,
      CASE
        WHEN zuora_subscription.current_term <= 12 THEN FALSE
        WHEN zuora_subscription.current_term > 12  THEN TRUE
        ELSE NULL
      END                                                               AS is_myb,
      zuora_subscription.current_term                                   AS current_term_months,
      ROUND(zuora_subscription.current_term / 12, 1)                    AS current_term_years,
      dim_crm_account_invoice.is_reseller,
      dim_product_detail.product_rate_plan_charge_name,
      dim_product_detail.product_tier_name                              AS product_category,
      dim_product_detail.product_delivery_type                          AS delivery,
      dim_product_detail.service_type,
      CASE
        WHEN LOWER(dim_product_detail.product_rate_plan_charge_name) LIKE '%edu or oss%'   THEN TRUE
        WHEN LOWER(dim_product_detail.product_rate_plan_charge_name) LIKE '%education%'    THEN TRUE
        WHEN LOWER(dim_product_detail.product_rate_plan_charge_name) LIKE '%y combinator%' THEN TRUE
        WHEN LOWER(dim_product_detail.product_rate_plan_charge_name) LIKE '%support%'      THEN TRUE
        WHEN LOWER(dim_product_detail.product_rate_plan_charge_name) LIKE '%reporter%'     THEN TRUE
        WHEN LOWER(dim_product_detail.product_rate_plan_charge_name) LIKE '%guest%'        THEN TRUE
        WHEN dim_product_detail.annual_billing_list_price = 0                              THEN TRUE
        ELSE FALSE
      END                                                               AS is_excluded_from_disc_analysis,
      dim_product_detail.annual_billing_list_price,
      ARRAY_AGG(IFF(zuora_subscription.created_by_id = '2c92a0fd55822b4d015593ac264767f2', -- All Self-Service / Web direct subscriptions are identified by that created_by_id
                   'Self-Service', 'Sales-Assisted'))                   AS subscription_sales_type,
      SUM(arr_agg.invoice_item_charge_amount)                           AS invoice_item_charge_amount,
      SUM(arr_agg.arr)/SUM(arr_agg.quantity)                            AS arpu,
      SUM(arr_agg.arr)                                                  AS arr,
      SUM(arr_agg.quantity)                                             AS quantity,
      {{ arr_buckets('SUM(arr_agg.arr)') }}                             AS arr_buckets,
      {{ number_of_seats_buckets('SUM(arr_agg.quantity)') }}            AS number_of_seats_buckets
    FROM arr_agg
    INNER JOIN zuora_subscription
      ON arr_agg.dim_subscription_id = zuora_subscription.subscription_id
    INNER JOIN dim_product_detail
      ON arr_agg.dim_product_detail_id = dim_product_detail.dim_product_detail_id
    INNER JOIN dim_billing_account
      ON arr_agg.dim_billing_account_id_invoice = dim_billing_account.dim_billing_account_id
    LEFT JOIN dim_crm_account AS dim_crm_account_invoice
      ON arr_agg.dim_crm_account_id_invoice = dim_crm_account_invoice.dim_crm_account_id
    LEFT JOIN dim_crm_account AS dim_crm_account_subscription
      ON arr_agg.dim_crm_account_id_subscription = dim_crm_account_subscription.dim_crm_account_id
    {{ dbt_utils.group_by(n=30) }}
    ORDER BY 3 DESC

), final AS (

    SELECT
      combined.*,
      CASE
        WHEN combined.current_term_months <= 12                                                              THEN 'Non-MYB'
        WHEN combined.current_term_months > 12  AND ABS(combined.invoice_item_charge_amount) > combined.arr  THEN 'Prepaid MYB'
        WHEN combined.current_term_months > 12  AND ABS(combined.invoice_item_charge_amount) <= combined.arr THEN 'Non Prepaid MYB'
        ELSE NULL
      END                                                               AS prepaid_myb,
      ABS(invoice_item_charge_amount) / (arr * current_term_years)
                                                                        AS pct_paid_of_total_revenue
    FROM combined

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@mcooperDD",
    created_date="2020-10-21",
    updated_date="2021-02-09",
) }}
