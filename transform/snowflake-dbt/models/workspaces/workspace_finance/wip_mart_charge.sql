WITH dim_amendment AS (

  SELECT *
  FROM {{ ref('dim_amendment') }}

), dim_billing_account AS (

  SELECT *
  FROM {{ ref('dim_billing_account') }}

), dim_charge AS (

  SELECT *
  FROM {{ ref('dim_charge') }}

), dim_crm_account AS (

  SELECT *
  FROM {{ ref('dim_crm_account') }}

), dim_product_detail AS (

  SELECT *
  FROM {{ ref('dim_product_detail') }}

), dim_subscription AS (

  SELECT *
  FROM {{ ref('dim_subscription') }}

), fct_charge AS (

    SELECT *
    FROM {{ ref('wip_fct_charge') }}

), mart_charge AS (

    SELECT
      --Surrogate Key
      dim_charge.dim_charge_id,

      --Natural Key
      dim_charge.subscription_name,
      dim_charge.subscription_version,
      dim_charge.rate_plan_charge_number,
      dim_charge.rate_plan_charge_version,
      dim_charge.rate_plan_charge_segment,

      --date info
      dim_subscription.subscription_start_date                                        AS subscription_start_date,
      dim_subscription.subscription_end_date                                          AS subscription_end_date,
      dim_subscription.subscription_start_month                                       AS subscription_start_month,
      dim_subscription.subscription_end_month                                         AS subscription_end_month,
      dim_charge.effective_start_date                                                 AS effective_start_date,
      dim_charge.effective_end_date                                                   AS effective_end_date,
      dim_charge.effective_start_month                                                AS effective_start_month,
      dim_charge.effective_end_month                                                  AS effective_end_month,
      dim_charge.created_date                                                         AS created_date,
      dim_charge.updated_date                                                         AS updated_date,

      --billing account info
      dim_billing_account.dim_billing_account_id                                      AS dim_billing_account_id,
      dim_billing_account.sold_to_country                                             AS sold_to_country,
      dim_billing_account.billing_account_name                                        AS billing_account_name,
      dim_billing_account.billing_account_number                                      AS billing_account_number,

      -- crm account info
      dim_crm_account.dim_crm_account_id                                              AS dim_crm_account_id,
      dim_crm_account.crm_account_name                                                AS crm_account_name,
      dim_crm_account.dim_parent_crm_account_id                                       AS dim_parent_crm_account_id,
      dim_crm_account.parent_crm_account_name                                         AS parent_crm_account_name,
      dim_crm_account.parent_crm_account_billing_country                              AS parent_crm_account_billing_country,
      dim_crm_account.parent_crm_account_sales_segment                                AS parent_crm_account_sales_segment,
      dim_crm_account.parent_crm_account_industry                                     AS parent_crm_account_industry,
      dim_crm_account.parent_crm_account_owner_team                                   AS parent_crm_account_owner_team,
      dim_crm_account.parent_crm_account_sales_territory                              AS parent_crm_account_sales_territory,
      dim_crm_account.parent_crm_account_tsp_region                                   AS parent_crm_account_tsp_region,
      dim_crm_account.parent_crm_account_tsp_sub_region                               AS parent_crm_account_tsp_sub_region,
      dim_crm_account.parent_crm_account_tsp_area                                     AS parent_crm_account_tsp_area,
      dim_crm_account.crm_account_tsp_region                                          AS crm_account_tsp_region,
      dim_crm_account.crm_account_tsp_sub_region                                      AS crm_account_tsp_sub_region,
      dim_crm_account.crm_account_tsp_area                                            AS crm_account_tsp_area,
      dim_crm_account.health_score                                                    AS health_score,
      dim_crm_account.health_score_color                                              AS health_score_color,
      dim_crm_account.health_number                                                   AS health_number,

      --subscription info
      dim_subscription.dim_subscription_id,
      dim_subscription.subscription_status                                            AS subscription_status,
      dim_subscription.subscription_sales_type                                        AS subscription_sales_type,
      dim_subscription.subscription_name_slugify                                      AS subscription_name_slugify,
      dim_subscription.oldest_subscription_in_cohort                                  AS oldest_subscription_in_cohort,
      dim_subscription.subscription_lineage                                           AS subscription_lineage,
      dim_subscription.subscription_cohort_month                                      AS subscription_cohort_month,
      dim_subscription.subscription_cohort_quarter                                    AS subscription_cohort_quarter,
      MIN(dim_subscription.subscription_cohort_month) OVER (
          PARTITION BY dim_billing_account.dim_billing_account_id)                    AS billing_account_cohort_month,
      MIN(dim_subscription.subscription_cohort_quarter) OVER (
          PARTITION BY dim_billing_account.dim_billing_account_id)                    AS billing_account_cohort_quarter,
      MIN(dim_subscription.subscription_cohort_month) OVER (
          PARTITION BY dim_crm_account.dim_crm_account_id)                            AS crm_account_cohort_month,
      MIN(dim_subscription.subscription_cohort_quarter) OVER (
          PARTITION BY dim_crm_account.dim_crm_account_id)                            AS crm_account_cohort_quarter,
      MIN(dim_subscription.subscription_cohort_month) OVER (
          PARTITION BY dim_crm_account.dim_parent_crm_account_id)                     AS parent_account_cohort_month,
      MIN(dim_subscription.subscription_cohort_quarter) OVER (
          PARTITION BY dim_crm_account.dim_parent_crm_account_id)                     AS parent_account_cohort_quarter,

      --Amendment Information
      dim_amendment_subscription.amendment_type                                       AS subscription_amendment_type,
      dim_amendment_charge.amendment_type                                             AS charge_amendment_type,

      --product info
      dim_product_detail.dim_product_detail_id,
      dim_product_detail.product_tier_name                                            AS product_tier_name,
      dim_product_detail.product_delivery_type                                        AS product_delivery_type,
      dim_product_detail.service_type                                                 AS service_type,
      dim_product_detail.product_rate_plan_name                                       AS product_rate_plan_name,

      --Additive Fields
      fct_charge.mrr,
      fct_charge.previous_mrr,
      fct_charge.delta_mrr,
      fct_charge.arr,
      fct_charge.previous_arr,
      fct_charge.delta_arr,
      fct_charge.quantity,
      fct_charge.previous_quantity,
      fct_charge.delta_quantity,
      fct_charge.tcv,
      fct_charge.previous_tcv,
      fct_charge.delta_tcv,

      --ARR Analysis Framework
      dim_charge.type_of_arr_change

    FROM fct_charge
    INNER JOIN dim_charge
      ON fct_charge.dim_charge_id = dim_charge.dim_charge_id
    INNER JOIN dim_subscription
      ON fct_charge.dim_subscription_id = dim_subscription.dim_subscription_id
    INNER JOIN dim_product_detail
      ON fct_charge.dim_product_detail_id = dim_product_detail.dim_product_detail_id
    INNER JOIN dim_billing_account
      ON fct_charge.dim_billing_account_id = dim_billing_account.dim_billing_account_id
    LEFT JOIN dim_crm_account
      ON dim_crm_account.dim_crm_account_id = dim_billing_account.dim_crm_account_id
    LEFT JOIN dim_amendment AS dim_amendment_subscription
      ON dim_subscription.dim_amendment_id_subscription = dim_amendment_subscription.dim_amendment_id
    LEFT JOIN dim_amendment AS dim_amendment_charge
      ON fct_charge.dim_amendment_id_charge = dim_amendment_charge.dim_amendment_id
    ORDER BY fct_charge.subscription_name, fct_charge.subscription_version, fct_charge.rate_plan_charge_number,
      fct_charge.rate_plan_charge_version, fct_charge.rate_plan_charge_segment

)

{{ dbt_audit(
    cte_ref="mart_charge",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-05-10",
    updated_date="2021-05-10"
) }}
