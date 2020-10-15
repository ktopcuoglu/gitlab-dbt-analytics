WITH dim_dates AS (

    SELECT *
    FROM {{ ref('dim_dates') }}

), dim_billing_accounts AS (

    SELECT *
    FROM {{ ref('dim_billing_accounts') }}

), dim_crm_accounts AS (

    SELECT *
    FROM {{ ref('dim_crm_accounts') }}

), dim_product_details AS (

    SELECT *
    FROM {{ ref('dim_product_details') }}

), fct_invoice_items AS (

    SELECT *
    FROM {{ ref('fct_invoice_items') }}
    WHERE is_last_segment_version = TRUE
      AND arr > 0

), zuora_subscription AS (

  SELECT *
  FROM {{ ref('zuora_subscription_source') }}
  WHERE is_deleted = FALSE
    AND exclude_from_analysis IN ('False', '')

), arr_month_by_month AS (

    SELECT
      dim_dates.date_actual                               AS arr_month,
      fct_invoice_items.invoice_date,
      fct_invoice_items.invoice_number,
      fct_invoice_items.effective_start_month,
      fct_invoice_items.effective_end_month,
      dim_billing_account_id_subscription,
      dim_crm_account_id_subscription,
      dim_billing_account_id_invoice,
      dim_crm_account_id_invoice,
      dim_subscription_id,
      dim_product_details_id,
      SUM(mrr)                                             AS mrr,
      SUM(arr)                                             AS arr,
      SUM(quantity)                                        AS quantity
    FROM fct_invoice_items
    INNER JOIN dim_dates
      ON fct_invoice_items.effective_start_month <= dim_dates.date_actual
      AND (fct_invoice_items.effective_end_month > dim_dates.date_actual
        OR fct_invoice_items.effective_end_month IS NULL)
      AND dim_dates.day_of_month = 1
    GROUP BY {{ dbt_utils.group_by(n=11) }}

)

SELECT
  arr_month,
  DATE_TRUNC('month',invoice_date)      AS invoice_month,
  invoice_number,
  ultimate_parent_account_id,
  ultimate_parent_account_name,
  ultimate_parent_billing_country,
  ultimate_parent_account_segment,
  is_reseller,
  product_rate_plan_charge_name,
  product_category,
  delivery,
  service_type,
  IFF(zuora_subscription.created_by_id = '2c92a0fd55822b4d015593ac264767f2', -- All Self-Service / Web direct subscriptions are identified by that created_by_id
      'Self-Service', 'Sales-Assisted') AS subscription_sales_type,
  effective_start_month,
  effective_end_month,
  annual_billing_list_price,
  arr/quantity                          AS arpu,
  arr                                   AS arr,
  quantity                              AS quantity
FROM arr_month_by_month
INNER JOIN zuora_subscription
  ON arr_month_by_month.dim_subscription_id = zuora_subscription.subscription_id
INNER JOIN dim_product_details
  ON arr_month_by_month.dim_product_details_id = dim_product_details.product_details_id
INNER JOIN dim_billing_accounts
  ON arr_month_by_month.dim_billing_account_id_invoice = dim_billing_accounts.billing_account_id
LEFT JOIN dim_crm_accounts
  ON arr_month_by_month.dim_crm_account_id_invoice = dim_crm_accounts.crm_account_id
WHERE lower(product_rate_plan_charge_name) NOT LIKE '%edu or oss%'
  AND lower(product_rate_plan_charge_name) NOT LIKE '%y combinator%'
  AND lower(product_rate_plan_charge_name) NOT LIKE '%support%'
  AND product_category IN ('Bronze','Silver','Gold','Ultimate','Premium','Starter')
  AND DATE_TRUNC('month',invoice_date) < DATE_TRUNC('month', CURRENT_DATE)
ORDER BY 2 DESC
