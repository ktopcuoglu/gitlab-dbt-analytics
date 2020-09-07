/* This table needs to be permanent to allow zero cloning at specific timestamps */
{{ config(materialized='table',
  transient=false)}}

WITH dim_accounts AS (

  SELECT *
  FROM {{ ref('dim_accounts') }}

), dim_customers AS (

  SELECT *
  FROM {{ ref('dim_customers') }}

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

), last_month_of_fiscal_quarter AS (

    SELECT DISTINCT
      DATE_TRUNC('month', last_day_of_fiscal_quarter) AS last_month_of_fiscal_quarter,
      fiscal_quarter_name_fy
    FROM {{ ref('dim_dates') }}

), last_month_of_fiscal_year AS (

    SELECT DISTINCT
      DATE_TRUNC('month', last_day_of_fiscal_year) AS last_month_of_fiscal_year,
      fiscal_year
    FROM {{ ref('dim_dates') }}

)

SELECT
  --primary_key
  fct_mrr.mrr_id,

  --date info
  dim_dates.date_actual AS arr_month,
  quarter.fiscal_quarter_name_fy,
  year.fiscal_year,
  dim_subscriptions.subscription_start_month,
  dim_subscriptions.subscription_end_month,

  --account info
  dim_accounts.account_id                                              AS zuora_account_id,
  dim_accounts.sold_to_country                                         AS zuora_sold_to_country,
  dim_accounts.account_name                                            AS zuora_account_name,
  dim_accounts.account_number                                          AS zuora_account_number,
  COALESCE(dim_customers.merged_to_account_id, dim_customers.crm_id)   AS crm_id,
  dim_customers.ultimate_parent_account_id,
  dim_customers.ultimate_parent_account_name,
  dim_customers.ultimate_parent_billing_country,
  dim_customers.ultimate_parent_account_segment,
  dim_customers.ultimate_parent_industry,
  dim_customers.ultimate_parent_account_owner_team,
  dim_customers.ultimate_parent_territory,

  --subscription info
  dim_subscriptions.subscription_name,
  dim_subscriptions.subscription_status,

  --product info
  dim_product_details.product_category,
  dim_product_details.delivery,
  dim_product_details.service_type,
  dim_product_details.product_rate_plan_name,
  --charge_type,
  fct_mrr.unit_of_measure,
  --array_agg(rate_plan_name)                       AS rate_plan_name,
  fct_mrr.mrr                                     AS mrr,
  fct_mrr.arr                                     AS arr,
  fct_mrr.quantity                                AS quantity
  FROM fct_mrr
  INNER JOIN dim_subscriptions
    ON dim_subscriptions.subscription_id = fct_mrr.subscription_id
  INNER JOIN dim_product_details
    ON dim_product_details.product_details_id = fct_mrr.product_details_id
  INNER JOIN dim_accounts
    ON dim_accounts.account_id = fct_mrr.account_id
  INNER JOIN dim_dates
    ON dim_dates.date_id = fct_mrr.date_id
  LEFT JOIN dim_customers
    ON dim_accounts.crm_id = dim_customers.crm_id
  LEFT JOIN last_month_of_fiscal_quarter quarter
    ON dim_dates.date_actual = quarter.last_month_of_fiscal_quarter
  LEFT JOIN last_month_of_fiscal_year year
    ON dim_dates.date_actual = year.last_month_of_fiscal_year
