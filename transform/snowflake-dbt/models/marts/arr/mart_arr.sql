/* This table needs to be permanent to allow zero cloning at specific timestamps */
{{ config(materialized='table',
  transient=false,
  schema="common_mart_sales")}}

WITH dim_billing_account AS (

  SELECT *
  FROM {{ ref('dim_billing_account') }}

), dim_crm_account AS (

  SELECT *
  FROM {{ ref('dim_crm_account') }}

), dim_date AS (

  SELECT *
  FROM {{ ref('dim_date') }}

), dim_product_detail AS (

  SELECT *
  FROM {{ ref('dim_product_detail') }}

), dim_subscription AS (

  SELECT *
  FROM {{ ref('dim_subscription') }}

), fct_mrr AS (

  SELECT *
  FROM {{ ref('fct_mrr') }}

), joined AS (

    SELECT
      --primary_key
      fct_mrr.mrr_id                                                                  AS primary_key,

      --date info
      dim_date.date_actual                                                            AS arr_month,
      IFF(is_first_day_of_last_month_of_fiscal_quarter, fiscal_quarter_name_fy, NULL) AS fiscal_quarter_name_fy,
      IFF(is_first_day_of_last_month_of_fiscal_year, fiscal_year, NULL)               AS fiscal_year,
      dim_subscription.subscription_start_month,
      dim_subscription.subscription_end_month,

      --account info
      dim_billing_account.dim_billing_account_id                                      AS dim_billing_account_id,
      dim_billing_account.sold_to_country                                             AS sold_to_country,
      dim_billing_account.billing_account_name                                        AS billing_account_name,
      dim_billing_account.billing_account_number                                      AS billing_account_number,
      dim_crm_account.dim_crm_account_id                                              AS dim_crm_account_id,
      dim_crm_account.crm_account_name,
      dim_crm_account.dim_parent_crm_account_id,
      dim_crm_account.parent_crm_account_name,
      dim_crm_account.parent_crm_account_billing_country,
      dim_crm_account.parent_crm_account_sales_segment,
      dim_crm_account.parent_crm_account_industry,
      dim_crm_account.parent_crm_account_owner_team,
      dim_crm_account.parent_crm_account_sales_territory,
      dim_crm_account.health_score,
      dim_crm_account.health_score_color,
      dim_crm_account.health_number,

      --subscription info
      dim_subscription.subscription_status,
      dim_subscription.subscription_sales_type,
      dim_subscription.subscription_name                                             AS subscription_name,
      dim_subscription.subscription_name_slugify                                     AS subscription_name_slugify,
      dim_subscription.oldest_subscription_in_cohort                                 AS oldest_subscription_in_cohort,
      dim_subscription.subscription_lineage                                          AS subscription_lineage,
      dim_subscription.subscription_cohort_month                                     AS subscription_cohort_month,
      dim_subscription.subscription_cohort_quarter                                   AS subscription_cohort_quarter,
      min(dim_subscription.subscription_cohort_month) OVER (
          PARTITION BY dim_billing_account.dim_billing_account_id)                   AS billing_account_cohort_month,
      min(dim_subscription.subscription_cohort_quarter) OVER (
          PARTITION BY dim_billing_account.dim_billing_account_id)                   AS billing_account_cohort_quarter,
      min(dim_subscription.subscription_cohort_month) OVER (
          PARTITION BY dim_crm_account.dim_crm_account_id)                           AS crm_account_cohort_month,
      min(dim_subscription.subscription_cohort_quarter) OVER (
          PARTITION BY dim_crm_account.dim_crm_account_id)                           AS crm_account_cohort_quarter,
      min(dim_subscription.subscription_cohort_month) OVER (
          PARTITION BY dim_crm_account.dim_parent_crm_account_id)                    AS parent_account_cohort_month,
      min(dim_subscription.subscription_cohort_quarter) OVER (
          PARTITION BY dim_crm_account.dim_parent_crm_account_id)                    AS parent_account_cohort_quarter,

      --product info
      dim_product_detail.product_tier_name                                           AS product_tier_name,
      dim_product_detail.product_delivery_type                                       AS product_delivery_type,
      dim_product_detail.service_type                                                AS service_type,
      dim_product_detail.product_rate_plan_name                                      AS product_rate_plan_name,
      --  not needed as all charges in fct_mrr are recurring
      --  fct_mrr.charge_type,
      fct_mrr.unit_of_measure,

      fct_mrr.mrr,
      fct_mrr.arr,
      fct_mrr.quantity

    FROM fct_mrr
    INNER JOIN dim_subscription
      ON dim_subscription.dim_subscription_id = fct_mrr.dim_subscription_id
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id
    INNER JOIN dim_billing_account
      ON dim_billing_account.dim_billing_account_id = fct_mrr.dim_billing_account_id
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr.dim_date_id
    LEFT JOIN dim_crm_account
      ON dim_billing_account.dim_crm_account_id = dim_crm_account.dim_crm_account_id

), final_table AS (

  SELECT
    joined.*,
    datediff(month, billing_account_cohort_month, arr_month)     AS months_since_billing_account_cohort_start,
    datediff(quarter, billing_account_cohort_quarter, arr_month) AS quarters_since_billing_account_cohort_start,
    datediff(month, crm_account_cohort_month, arr_month)         AS months_since_crm_account_cohort_start,
    datediff(quarter, crm_account_cohort_quarter, arr_month)     AS quarters_since_crm_account_cohort_start,
    datediff(month, parent_account_cohort_month, arr_month)      AS months_since_parent_account_cohort_start,
    datediff(quarter, parent_account_cohort_quarter, arr_month)  AS quarters_since_parent_account_cohort_start,
    datediff(month, subscription_cohort_month, arr_month)        AS months_since_subscription_cohort_start,
    datediff(quarter, subscription_cohort_quarter, arr_month)    AS quarters_since_subscription_cohort_start
  FROM joined

)

{{ dbt_audit(
    cte_ref="final_table",
    created_by="@msendal",
    updated_by="@paul_armstrong",
    created_date="2020-09-04",
    updated_date="2021-02-22"
) }}