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
      AND arr != 0

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
      fct_invoice_items.invoice_item_id,
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
      --filter out 2 subscription_ids with known data quality issues when comparing invoiced subscriptions to the Zuora UI.
      AND dim_subscription_id NOT IN ('2c92a0ff5e1dcf14015e3c191d4f7689','2c92a00e6a3477b5016a46aaec2f08bc')
    {{ dbt_utils.group_by(n=12) }}

), final AS (

    SELECT
      {{ dbt_utils.surrogate_key(['arr_month_by_month.arr_month', 'arr_month_by_month.invoice_item_id']) }}
                                                                        AS invoice_item_month_id,
      arr_month_by_month.arr_month,
      DATE_TRUNC('month',arr_month_by_month.invoice_date)               AS invoice_month,
      arr_month_by_month.invoice_number,
      dim_crm_accounts_invoice.ultimate_parent_account_id               AS parent_account_id_invoice,
      dim_crm_accounts_invoice.ultimate_parent_account_name             AS parent_account_name_invoice,
      dim_crm_accounts_invoice.ultimate_parent_billing_country          AS parent_billing_country_invoice,
      dim_crm_accounts_invoice.ultimate_parent_account_segment          AS parent_account_segment_invoice,
      dim_crm_accounts_invoice.crm_account_id                           AS crm_account_id_invoice,
      dim_crm_accounts_invoice.crm_account_name                         AS crm_account_name_invoice,
      dim_crm_accounts_subscription.ultimate_parent_account_id          AS parent_account_id_subscription,
      dim_crm_accounts_subscription.ultimate_parent_account_name        AS parent_account_name_subscription,
      dim_crm_accounts_subscription.ultimate_parent_billing_country     AS parent_billing_country_subscription,
      dim_crm_accounts_subscription.ultimate_parent_account_segment     AS parent_account_segment_subscription,
      dim_crm_accounts_subscription.crm_account_id                      AS crm_account_id_subscription,
      dim_crm_accounts_subscription.crm_account_name                    AS crm_account_name_subscription,
      zuora_subscription.subscription_name,
      dim_crm_accounts_invoice.is_reseller,
      dim_product_details.product_rate_plan_charge_name,
      dim_product_details.product_category,
      dim_product_details.delivery,
      dim_product_details.service_type,
      IFF(zuora_subscription.created_by_id = '2c92a0fd55822b4d015593ac264767f2', -- All Self-Service / Web direct subscriptions are identified by that created_by_id
        'Self-Service', 'Sales-Assisted') AS subscription_sales_type,
      CASE
        WHEN LOWER(dim_product_details.product_rate_plan_charge_name) LIKE '%edu or oss%'   THEN TRUE
        WHEN LOWER(dim_product_details.product_rate_plan_charge_name) LIKE '%y combinator%' THEN TRUE
        WHEN LOWER(dim_product_details.product_rate_plan_charge_name) LIKE '%support%'      THEN TRUE
        ELSE FALSE
      END                                                               AS is_excluded_from_disc_analysis,
      arr_month_by_month.effective_start_month,
      arr_month_by_month.effective_end_month,
      zuora_subscription.subscription_start_date,
      zuora_subscription.subscription_end_date,
      dim_product_details.annual_billing_list_price,
      arr_month_by_month.arr/arr_month_by_month.quantity                AS arpu,
      arr_month_by_month.arr                                            AS arr,
      arr_month_by_month.quantity                                       AS quantity
    FROM arr_month_by_month
    INNER JOIN zuora_subscription
      ON arr_month_by_month.dim_subscription_id = zuora_subscription.subscription_id
    INNER JOIN dim_product_details
      ON arr_month_by_month.dim_product_details_id = dim_product_details.product_details_id
    INNER JOIN dim_billing_accounts
      ON arr_month_by_month.dim_billing_account_id_invoice = dim_billing_accounts.billing_account_id
    LEFT JOIN dim_crm_accounts AS dim_crm_accounts_invoice
      ON arr_month_by_month.dim_crm_account_id_invoice = dim_crm_accounts_invoice.crm_account_id
    LEFT JOIN dim_crm_accounts AS dim_crm_accounts_subscription
      ON arr_month_by_month.dim_crm_account_id_subscription = dim_crm_accounts_subscription.crm_account_id
    ORDER BY 3 DESC

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2020-10-21",
    updated_date="2020-10-21",
) }}
