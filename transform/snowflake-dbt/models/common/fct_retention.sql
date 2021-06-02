WITH dim_date AS (

    SELECT *
    FROM {{ ref('dim_date') }}

), dim_crm_account AS (

    SELECT *
    FROM {{ ref('prep_crm_account') }}

), dim_product_detail AS (

    SELECT *
    FROM {{ ref('prep_product_detail') }}

), dim_subscription AS (

    SELECT *
    FROM {{ ref('prep_subscription') }}

), fct_mrr AS (

    SELECT *
    FROM {{ ref('prep_recurring_charge') }}
    WHERE subscription_status IN ('Active', 'Cancelled')

), next_renewal_month AS (

    SELECT DISTINCT
      merged_accounts.dim_parent_crm_account_id,
      MIN(subscription_end_month) OVER (PARTITION BY merged_accounts.dim_parent_crm_account_id)    AS next_renewal_month
    FROM dim_subscription
    INNER JOIN dim_date
      ON dim_date.date_actual = dim_subscription.subscription_end_month
    LEFT JOIN dim_crm_account AS crm_accounts
      ON crm_accounts.dim_crm_account_id = dim_subscription.dim_crm_account_id
    INNER JOIN dim_crm_account AS merged_accounts
      ON merged_accounts.dim_crm_account_id = COALESCE(crm_accounts.merged_to_account_id, crm_accounts.dim_crm_account_id)
    WHERE subscription_end_month >= DATE_TRUNC('month',CURRENT_DATE)
      AND subscription_end_month <= DATEADD('year', 1, date_actual)

), last_renewal_month AS (

    SELECT DISTINCT
      merged_accounts.dim_parent_crm_account_id,
      MAX(subscription_end_month) OVER (PARTITION BY merged_accounts.dim_parent_crm_account_id)    AS last_renewal_month
    FROM dim_subscription
    INNER JOIN dim_date
      ON dim_date.date_actual = dim_subscription.subscription_end_month
    LEFT JOIN dim_crm_account AS crm_accounts
      ON crm_accounts.dim_crm_account_id = dim_subscription.dim_crm_account_id
    INNER JOIN dim_crm_account AS merged_accounts
      ON merged_accounts.dim_crm_account_id = COALESCE(crm_accounts.merged_to_account_id, crm_accounts.dim_crm_account_id)
    WHERE subscription_end_month < DATE_TRUNC('month',CURRENT_DATE)
      AND subscription_end_month <= DATEADD('year', 1, date_actual)

), parent_account_mrrs AS (

    SELECT
      dim_crm_account.dim_parent_crm_account_id,
      dim_date.date_actual                                      AS mrr_month,
      dateadd('year', 1, date_actual)                           AS retention_month,
      next_renewal_month,
      last_renewal_month
    FROM fct_mrr
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_mrr.dim_product_detail_id
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr.dim_date_id
    LEFT JOIN dim_crm_account
      ON dim_crm_account.dim_crm_account_id = fct_mrr.dim_crm_account_id
    LEFT JOIN next_renewal_month
      ON next_renewal_month.dim_parent_crm_account_id = dim_crm_account.dim_parent_crm_account_id
    LEFT JOIN last_renewal_month
      ON last_renewal_month.dim_parent_crm_account_id = dim_crm_account.dim_parent_crm_account_id

), retention_subs AS (

    SELECT dim_parent_crm_account_id,
           retention_month
    FROM parent_account_mrrs

)

SELECT *
FROM retention_subs
