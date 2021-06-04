WITH dim_date AS (

    SELECT *
    FROM {{ ref('dim_date') }}

), prep_crm_account AS (

    SELECT *
    FROM {{ ref('prep_crm_account') }}

), prep_product_detail AS (

    SELECT *
    FROM {{ ref('prep_product_detail') }}

), prep_subscription AS (

    SELECT *
    FROM {{ ref('prep_subscription') }}

), prep_recurring_charge AS (

    SELECT *
    FROM {{ ref('prep_recurring_charge') }}
    WHERE subscription_status IN ('Active', 'Cancelled')
    -- Exclude EDU & OSS
    AND mrr != 0

), next_renewal_month AS (

    SELECT DISTINCT
      merged_accounts.dim_parent_crm_account_id                                                                       AS dim_parent_crm_account_id,
      prep_product_detail.product_tier_name                                                                           AS product_tier_name,
      MIN(subscription_end_month) OVER (PARTITION BY merged_accounts.dim_parent_crm_account_id)                       AS next_renewal_month,
      MIN(subscription_end_month) OVER (PARTITION BY merged_accounts.dim_parent_crm_account_id, product_tier_name)    AS next_renewal_month_product
    FROM prep_recurring_charge
    INNER JOIN dim_date
      ON dim_date.date_id = prep_recurring_charge.dim_date_id
    LEFT JOIN prep_crm_account AS crm_accounts
      ON crm_accounts.dim_crm_account_id = prep_recurring_charge.dim_crm_account_id
    INNER JOIN prep_crm_account AS merged_accounts
      ON merged_accounts.dim_crm_account_id = COALESCE(crm_accounts.merged_to_account_id, crm_accounts.dim_crm_account_id)
    LEFT JOIN prep_subscription
      ON prep_subscription.dim_subscription_id = prep_recurring_charge.dim_subscription_id
    INNER JOIN prep_product_detail
      ON prep_product_detail.dim_product_detail_id = prep_recurring_charge.dim_product_detail_id
    WHERE subscription_end_month >= DATE_TRUNC('month',CURRENT_DATE)
      AND subscription_end_month <= DATEADD('year', 1, date_actual)

), last_renewal_month AS (

    SELECT DISTINCT
      merged_accounts.dim_parent_crm_account_id                                                                       AS dim_parent_crm_account_id,
      prep_product_detail.product_tier_name                                                                           AS product_tier_name,
      MAX(subscription_end_month) OVER (PARTITION BY merged_accounts.dim_parent_crm_account_id)                       AS last_renewal_month,
      MAX(subscription_end_month) OVER (PARTITION BY merged_accounts.dim_parent_crm_account_id, product_tier_name)    AS last_renewal_month_product
    FROM prep_recurring_charge
    INNER JOIN dim_date
      ON dim_date.date_id = prep_recurring_charge.dim_date_id
    LEFT JOIN prep_crm_account AS crm_accounts
      ON crm_accounts.dim_crm_account_id = prep_recurring_charge.dim_crm_account_id
    INNER JOIN prep_crm_account AS merged_accounts
      ON merged_accounts.dim_crm_account_id = COALESCE(crm_accounts.merged_to_account_id, crm_accounts.dim_crm_account_id)
    LEFT JOIN prep_subscription
      ON prep_subscription.dim_subscription_id = prep_recurring_charge.dim_subscription_id
    INNER JOIN prep_product_detail
      ON prep_product_detail.dim_product_detail_id = prep_recurring_charge.dim_product_detail_id
    WHERE subscription_end_month < DATE_TRUNC('month',CURRENT_DATE)
      AND subscription_end_month <= DATEADD('year', 1, date_actual)

), final AS (

    SELECT
      prep_crm_account.dim_parent_crm_account_id                AS dim_parent_crm_account_id,
      dim_date.date_actual                                      AS mrr_month,
      dateadd('year', 1, date_actual)                           AS retention_month,
      next_renewal_month_product                                AS next_renewal_month_product,
      last_renewal_month_product                                AS last_renewal_month_product,
      next_renewal_month                                        AS next_renewal_month,
      last_renewal_month                                        AS last_renewal_month,
      ZEROIFNULL(mrr)                                           AS mrr,
      ZEROIFNULL(arr)                                           AS arr,
      ZEROIFNULL(quantity)                                      AS quantity,
      prep_product_detail.product_tier_name                     AS product_tier_name,
      prep_product_detail.product_ranking                       AS product_ranking
    FROM prep_recurring_charge
    INNER JOIN prep_product_detail
      ON prep_product_detail.dim_product_detail_id = prep_recurring_charge.dim_product_detail_id
    INNER JOIN dim_date
      ON dim_date.date_id = prep_recurring_charge.dim_date_id
    LEFT JOIN prep_crm_account
      ON prep_crm_account.dim_crm_account_id = prep_recurring_charge.dim_crm_account_id
    LEFT JOIN next_renewal_month
      ON next_renewal_month.dim_parent_crm_account_id = prep_crm_account.dim_parent_crm_account_id
    LEFT JOIN last_renewal_month
      ON last_renewal_month.dim_parent_crm_account_id = prep_crm_account.dim_parent_crm_account_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2021-06-02",
    updated_date="2021-06-02"
) }}
