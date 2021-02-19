WITH dates AS (

    SELECT * FROM {{ ref('dim_date') }}

), mrr_totals AS (

    SELECT * FROM {{ ref('fct_mrr') }}

), subscription AS (

    SELECT * FROM {{ ref('dim_subscription') }}

), billing_account AS (

    SELECT * FROM {{ ref('dim_billing_account') }}

), crm_account AS (

    SELECT * FROM {{ ref('dim_crm_account') }}

), product_detail AS (

    SELECT * FROM {{ ref('dim_product_detail') }}

), map_product_tier AS (

    SELECT * FROM {{ ref('map_product_tier') }}

), joined AS (

      SELECT
           mrr_totals.mrr_id                                                AS mrr_id,
           mrr_totals.dim_billing_account_id                                AS dim_billing_account_id,
           mrr_totals.dim_crm_account_id                                    AS dim_crm_account_id,
           mrr_totals.dim_subscription_id                                   AS dim_subscription_id,
           mrr_totals.dim_product_detail_id                                 AS dim_product_detail_id,
           mrr_totals.dim_date_id                                           AS dim_date_id,
           mrr_totals.mrr                                                   AS mrr,
           mrr_totals.arr                                                   AS arr,
           mrr_totals.quantity                                              AS quantity,
           mrr_totals.unit_of_measure                                       AS unit_of_measure,
           map_product_tier.product_delivery_type                           AS product_category,
           map_product_tier.product_tier                                    AS delivery,
           billing_account.billing_account_name                             AS billing_account_name,
           billing_account.billing_account_number                           AS billing_account_number,
           crm_account.crm_account_name                                     AS crm_account_name,
           crm_account.ultimate_parent_account_id                           AS ultimate_parent_account_id,
           crm_account.ultimate_parent_account_name                         AS ultimate_parent_account_name,
           subscription.subscription_name                                   AS subscription_name,
           subscription.subscription_name_slugify                           AS subscription_name_slugify,
           subscription.oldest_subscription_in_cohort                       AS oldest_subscription_in_cohort,
           subscription.lineage                                             AS subscription_lineage,
           subscription.cohort_month                                        AS subscription_cohort_month,
           subscription.cohort_quarter                                      AS subscription_cohort_quarter,
           min(subscription.cohort_month) OVER (
              PARTITION BY billing_account.dim_billing_account_id)          AS billing_account_cohort_month,
           min(subscription.cohort_quarter) OVER (
              PARTITION BY billing_account.dim_billing_account_id)          AS billing_account_cohort_quarter,
           min(subscription.cohort_month) OVER (
              PARTITION BY crm_account.crm_account_id)                      AS crm_account_cohort_month,
           min(subscription.cohort_quarter) OVER (
              PARTITION BY crm_account.crm_account_id)                      AS crm_account_cohort_quarter,
           min(subscription.cohort_month) OVER (
              PARTITION BY crm_account.ultimate_parent_account_id)          AS parent_account_cohort_month,
           min(subscription.cohort_quarter) OVER (
              PARTITION BY crm_account.ultimate_parent_account_id)          AS parent_account_cohort_quarter
    FROM mrr_totals
    JOIN subscription
    ON subscription.dim_subscription_id = mrr_totals.dim_subscription_id
    JOIN product_detail
    ON product_detail.dim_product_detail_id = mrr_totals.dim_product_detail_id
    JOIN billing_account
    ON billing_account.dim_billing_account_id = mrr_totals.dim_billing_account_id
    JOIN crm_account
    ON billing_account.dim_crm_account_id = crm_account.crm_account_id
    JOIN map_product_tier
    ON map_product_tier.product_rate_plan_id = product_detail.product_rate_plan_id
    WHERE mrr_totals.dim_billing_account_id NOT IN ({{ zuora_excluded_accounts() }})
    AND mrr_totals.mrr > 0

), final_table AS (

   SELECT joined.*,
      datediff(month, billing_account_cohort_month, dates.date_day)     AS months_since_billing_account_cohort_start,
      datediff(quarter, billing_account_cohort_quarter, dates.date_day) AS quarters_since_billing_account_cohort_start,
      datediff(month, crm_account_cohort_month, dates.date_day)         AS months_since_crm_account_cohort_start,
      datediff(quarter, crm_account_cohort_quarter, dates.date_day)     AS quarters_since_crm_account_cohort_start,
      datediff(month, parent_account_cohort_month, dates.date_day)      AS months_since_parent_account_cohort_start,
      datediff(quarter, parent_account_cohort_quarter, dates.date_day)  AS quarters_since_parent_account_cohort_start,
      datediff(month, subscription_cohort_month, dates.date_day)        AS months_since_subscription_cohort_start,
      datediff(quarter, subscription_cohort_quarter, dates.date_day)    AS quarters_since_subscription_cohort_start
    FROM joined
    JOIN dates ON dates.date_id = joined.dim_date_id
    WHERE billing_account_cohort_month IS NOT NULL
    AND subscription_cohort_month IS NOT NULL
)

{{ dbt_audit(
    cte_ref="final_table",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2021-01-07",
    updated_date="2021-01-07"
) }}