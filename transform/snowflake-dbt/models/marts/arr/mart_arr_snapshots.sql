{{ config({
        "materialized": "incremental",
        "unique_key": "primary_key",
        "tags": ["arr_snapshots"]
    })
}}

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

), dim_subscriptions_snapshots AS (

    SELECT *
    FROM {{ ref('dim_subscriptions_snapshots') }}

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    WHERE snapshot_id > (SELECT max(dim_dates.date_id)
                            FROM {{ this }}
                            INNER JOIN dim_dates
                            ON dim_dates.date_actual = snapshot_date
                            )

    {% endif %}

), fct_mrr_snapshots AS (

    SELECT *
    FROM {{ ref('fct_mrr_snapshots') }}

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    WHERE snapshot_id > (SELECT max(dim_dates.date_id)
                            FROM {{ this }}
                            INNER JOIN dim_dates
                            ON dim_dates.date_actual = snapshot_date
                            )

    {% endif %}

), final AS (

    SELECT
      --primary_key
      fct_mrr_snapshots.mrr_snapshot_id                                                AS primary_key,
      fct_mrr_snapshots.mrr_id,

      --date info
      snapshot_dates.date_actual                                                       AS snapshot_date,
      arr_month.date_actual                                                            AS arr_month,
      IFF(arr_month.is_first_day_of_last_month_of_fiscal_quarter, arr_month.fiscal_quarter_name_fy, NULL)
        AS fiscal_quarter_name_fy,
      IFF(arr_month.is_first_day_of_last_month_of_fiscal_year, arr_month.fiscal_year, NULL)
        AS fiscal_year,
      dim_subscriptions_snapshots.subscription_start_month,
      dim_subscriptions_snapshots.subscription_end_month,

      --account info
      dim_billing_accounts.billing_account_id                                          AS zuora_account_id,
      dim_billing_accounts.sold_to_country                                             AS zuora_sold_to_country,
      dim_billing_accounts.billing_account_name                                        AS zuora_account_name,
      dim_billing_accounts.billing_account_number                                      AS zuora_account_number,
      COALESCE(dim_crm_accounts.merged_to_account_id, dim_crm_accounts.crm_account_id) AS crm_id,
      dim_crm_accounts.ultimate_parent_account_id,
      dim_crm_accounts.ultimate_parent_account_name,
      dim_crm_accounts.ultimate_parent_billing_country,
      dim_crm_accounts.ultimate_parent_account_segment,
      dim_crm_accounts.ultimate_parent_industry,
      dim_crm_accounts.ultimate_parent_account_owner_team,
      dim_crm_accounts.ultimate_parent_territory,
      dim_crm_accounts.is_reseller,

      --subscription info
      dim_subscriptions_snapshots.subscription_name,
      dim_subscriptions_snapshots.subscription_status,
      dim_subscriptions_snapshots.subscription_sales_type,

      --product info
      dim_product_details.product_category,
      dim_product_details.delivery,
      dim_product_details.service_type,
      dim_product_details.product_rate_plan_name                                        AS rate_plan_name,
      --  not needed as all charges in fct_mrr are recurring
      --  fct_mrr.charge_type,
      fct_mrr_snapshots.unit_of_measure,

      fct_mrr_snapshots.mrr,
      fct_mrr_snapshots.arr,
      fct_mrr_snapshots.quantity
    FROM fct_mrr_snapshots
    INNER JOIN dim_subscriptions_snapshots
      ON dim_subscriptions_snapshots.subscription_id = fct_mrr_snapshots.subscription_id
      AND dim_subscriptions_snapshots.snapshot_id = fct_mrr_snapshots.snapshot_id
    INNER JOIN dim_product_details
      ON dim_product_details.product_details_id = fct_mrr_snapshots.product_details_id
    INNER JOIN dim_billing_accounts
      ON dim_billing_accounts.billing_account_id= fct_mrr_snapshots.billing_account_id
    INNER JOIN dim_dates AS arr_month
      ON arr_month.date_id = fct_mrr_snapshots.date_id
    INNER JOIN dim_dates AS snapshot_dates
      ON snapshot_dates.date_id = fct_mrr_snapshots.snapshot_id
    LEFT JOIN dim_crm_accounts
        ON dim_billing_accounts.crm_account_id = dim_crm_accounts.crm_account_id

)

SELECT *
FROM final


