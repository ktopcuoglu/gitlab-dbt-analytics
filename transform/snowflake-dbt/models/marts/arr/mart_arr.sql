/* This table needs to be permanent to allow zero cloning at specific timestamps */
{{ config(materialized='table',
  transient=false)}}

WITH dim_billing_accounts AS (

  SELECT *
  FROM {{ ref('dim_billing_accounts') }}

), dim_crm_accounts AS (

  SELECT *
  FROM {{ ref('dim_crm_accounts') }}

), dim_dates AS (

  SELECT *
  FROM {{ ref('dim_dates') }}

), dim_product_details AS (

  SELECT *
  FROM {{ ref('dim_product_details') }}

), dim_subscriptions AS (

  SELECT *
  FROM {{ ref('dim_subscriptions') }}

), fct_mrr AS (

  SELECT *
  FROM {{ ref('fct_mrr') }}

)

SELECT
  --primary_key
  fct_mrr.mrr_id AS primary_key,

  --date info
  dim_dates.date_actual AS arr_month,
  IFF(is_first_day_of_last_month_of_fiscal_quarter, fiscal_quarter_name_fy, NULL)       AS fiscal_quarter_name_fy,
  IFF(is_first_day_of_last_month_of_fiscal_year, fiscal_year, NULL)                     AS fiscal_year,
  dim_subscriptions.subscription_start_month,
  dim_subscriptions.subscription_end_month,

  --account info
  dim_billing_accounts.billing_account_id                                               AS zuora_account_id,
  dim_billing_accounts.sold_to_country                                                  AS zuora_sold_to_country,
  dim_billing_accounts.billing_account_name                                             AS zuora_account_name,
  dim_billing_accounts.billing_account_number                                           AS zuora_account_number,
  dim_crm_accounts.crm_account_id                                                       AS crm_id,
  dim_crm_accounts.crm_account_name,
  dim_crm_accounts.ultimate_parent_account_id,
  dim_crm_accounts.ultimate_parent_account_name,
  dim_crm_accounts.ultimate_parent_billing_country,
  dim_crm_accounts.ultimate_parent_account_segment,
  dim_crm_accounts.ultimate_parent_industry,
  dim_crm_accounts.ultimate_parent_account_owner_team,
  dim_crm_accounts.ultimate_parent_territory,

  --subscription info
  dim_subscriptions.subscription_name,
  dim_subscriptions.subscription_status,
  dim_subscriptions.subscription_sales_type,

  --product info
  dim_product_details.product_category,
  dim_product_details.delivery,
  dim_product_details.service_type,
  dim_product_details.product_rate_plan_name                            AS rate_plan_name,
  --  not needed as all charges in fct_mrr are recurring
  --  fct_mrr.charge_type,
  fct_mrr.unit_of_measure,

  fct_mrr.mrr,
  fct_mrr.arr,
  fct_mrr.quantity
  FROM fct_mrr
  INNER JOIN dim_subscriptions
    ON dim_subscriptions.subscription_id = fct_mrr.subscription_id
  INNER JOIN dim_product_details
    ON dim_product_details.product_details_id = fct_mrr.product_details_id
  INNER JOIN dim_billing_accounts
    ON dim_billing_accounts.billing_account_id= fct_mrr.billing_account_id
  INNER JOIN dim_dates
    ON dim_dates.date_id = fct_mrr.date_id
  LEFT JOIN dim_crm_accounts
    ON dim_billing_accounts.crm_account_id = dim_crm_accounts.crm_account_id
