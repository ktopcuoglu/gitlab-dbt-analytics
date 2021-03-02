{{ config({
        "materialized": "incremental",
        "unique_key": "primary_key",
        "tags": ["arr_snapshots"]
    })
}}

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

), dim_subscriptions_snapshots AS (

    SELECT *
    FROM {{ ref('dim_subscriptions_snapshots') }}

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    WHERE snapshot_id > (SELECT max(dim_date.date_id)
                            FROM {{ this }}
                            INNER JOIN dim_date
                            ON dim_date.date_actual = snapshot_date
                            )

    {% endif %}

), fct_mrr_snapshots AS (

    SELECT *
    FROM {{ ref('fct_mrr_snapshots') }}

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    WHERE snapshot_id > (SELECT max(dim_date.date_id)
                            FROM {{ this }}
                            INNER JOIN dim_date
                            ON dim_date.date_actual = snapshot_date
                            )

    {% endif %}

), joined AS (

    SELECT
      --primary_key
      fct_mrr_snapshots.mrr_snapshot_id                                                     AS primary_key,
      fct_mrr_snapshots.mrr_id                                                              AS mrr_id,

      --date info
      snapshot_dates.date_actual                                                            AS snapshot_date,
      arr_month.date_actual                                                                 AS arr_month,
      IFF(arr_month.is_first_day_of_last_month_of_fiscal_quarter, arr_month.fiscal_quarter_name_fy, NULL)
        AS fiscal_quarter_name_fy,
      IFF(arr_month.is_first_day_of_last_month_of_fiscal_year, arr_month.fiscal_year, NULL)
        AS fiscal_year,
      dim_subscriptions_snapshots.subscription_start_month                                  AS subscription_start_month,
      dim_subscriptions_snapshots.subscription_end_month                                    AS subscription_end_month,
      dim_subscriptions_snapshots.subscription_end_date                                     AS subscription_end_date,

      --account info
      dim_billing_account.dim_billing_account_id                                            AS dim_billing_account_id,
      dim_billing_account.sold_to_country                                                   AS sold_to_country,
      dim_billing_account.billing_account_name                                              AS billing_account_name,
      dim_billing_account.billing_account_number                                            AS billing_account_number,
      COALESCE(dim_crm_account.merged_to_account_id, dim_crm_account.dim_crm_account_id)    AS dim_crm_account_id,
      dim_crm_account.crm_account_name                                                      AS crm_account_name,
      dim_crm_account.dim_parent_crm_account_id                                             AS dim_parent_crm_account_id,
      dim_crm_account.parent_crm_account_name                                               AS parent_crm_account_name,
      dim_crm_account.parent_crm_account_billing_country                                    AS parent_crm_account_billing_country,
      dim_crm_account.parent_crm_account_sales_segment                                      AS parent_crm_account_sales_segment,
      dim_crm_account.parent_crm_account_industry                                           AS parent_crm_account_industry,
      dim_crm_account.parent_crm_account_owner_team                                         AS parent_crm_account_owner_team,
      dim_crm_account.parent_crm_account_sales_territory                                    AS parent_crm_account_sales_territory,
      dim_crm_account.health_score                                                          AS health_score,
      dim_crm_account.health_score_color                                                    AS health_score_color,
      dim_crm_account.health_number                                                         AS health_number,

      --subscription info
      dim_subscriptions_snapshots.subscription_status                                        AS subscription_status,
      dim_subscriptions_snapshots.subscription_sales_type                                    AS subscription_sales_type,
      dim_subscriptions_snapshots.subscription_name                                          AS subscription_name,
      dim_subscriptions_snapshots.subscription_name_slugify                                  AS subscription_name_slugify,
      dim_subscriptions_snapshots.oldest_subscription_in_cohort                              AS oldest_subscription_in_cohort,
      dim_subscriptions_snapshots.subscription_lineage                                       AS subscription_lineage,
      dim_subscriptions_snapshots.subscription_cohort_month                                  AS subscription_cohort_month,
      dim_subscriptions_snapshots.subscription_cohort_quarter                                AS subscription_cohort_quarter,
      min(dim_subscriptions_snapshots.subscription_cohort_month) OVER (
          PARTITION BY dim_billing_account.dim_billing_account_id)                          AS billing_account_cohort_month,
      min(dim_subscriptions_snapshots.subscription_cohort_quarter) OVER (
          PARTITION BY dim_billing_account.dim_billing_account_id)                          AS billing_account_cohort_quarter,
      min(dim_subscriptions_snapshots.subscription_cohort_month) OVER (
          PARTITION BY dim_crm_account.dim_crm_account_id)                                  AS crm_account_cohort_month,
      min(dim_subscriptions_snapshots.subscription_cohort_quarter) OVER (
          PARTITION BY dim_crm_account.dim_crm_account_id)                                  AS crm_account_cohort_quarter,
      min(dim_subscriptions_snapshots.subscription_cohort_month) OVER (
          PARTITION BY dim_crm_account.dim_parent_crm_account_id)                           AS parent_account_cohort_month,
      min(dim_subscriptions_snapshots.subscription_cohort_quarter) OVER (
          PARTITION BY dim_crm_account.dim_parent_crm_account_id)                           AS parent_account_cohort_quarter,

      --product info
      dim_product_detail.product_tier_name                                                  AS product_tier_name,
      dim_product_detail.product_delivery_type                                              AS product_delivery_type,
      dim_product_detail.service_type                                                       AS service_type,
      dim_product_detail.product_rate_plan_name                                             AS product_rate_plan_name,

      -- MRR values
      --  not needed as all charges in fct_mrr are recurring
      --  fct_mrr.charge_type,
      fct_mrr_snapshots.unit_of_measure                                                     AS unit_of_measure,
      fct_mrr_snapshots.mrr                                                                 AS mrr,
      fct_mrr_snapshots.arr                                                                 AS arr,
      fct_mrr_snapshots.quantity                                                            AS quantity
    FROM fct_mrr_snapshots
    INNER JOIN dim_subscriptions_snapshots
      ON dim_subscriptions_snapshots.subscription_id = fct_mrr_snapshots.subscription_id
      AND dim_subscriptions_snapshots.snapshot_id = fct_mrr_snapshots.snapshot_id
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_mrr_snapshots.product_details_id
    INNER JOIN dim_billing_account
      ON dim_billing_account.dim_billing_account_id = fct_mrr_snapshots.billing_account_id
    INNER JOIN dim_date AS arr_month
      ON arr_month.date_id = fct_mrr_snapshots.date_id
    INNER JOIN dim_date AS snapshot_dates
      ON snapshot_dates.date_id = fct_mrr_snapshots.snapshot_id
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

SELECT *
FROM final_table
