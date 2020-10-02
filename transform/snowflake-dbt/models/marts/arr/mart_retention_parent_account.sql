WITH dim_crm_accounts AS (

    SELECT *
    FROM {{ ref('dim_crm_accounts') }}

), dim_dates AS (

    SELECT *
    FROM {{ ref('dim_dates') }}

), fct_mrr AS (

    SELECT *
    FROM {{ ref('fct_mrr') }}

), parent_account_mrrs AS (

    SELECT
      dim_crm_accounts.ultimate_parent_account_id,
      dim_dates.date_actual           AS mrr_month,
      dateadd('year', 1, date_actual) AS retention_month,
      SUM(mrr)                        AS mrr_total
    FROM fct_mrr
    LEFT JOIN dim_crm_accounts
      ON dim_crm_accounts.crm_account_id = fct_mrr.crm_account_id
    INNER JOIN dim_dates
      ON dim_dates.date_id = fct_mrr.date_id
    GROUP BY 1, 2, 3

), retention_subs AS (

    SELECT
      current_mrr.ultimate_parent_account_id,
      current_mrr.mrr_month     AS original_mrr_month,
      current_mrr.retention_month,
      current_mrr.mrr_total     AS original_mrr,
      future_mrr.mrr_total      AS retention_mrr
    FROM parent_account_mrrs AS current_mrr
    LEFT JOIN parent_account_mrrs AS future_mrr
      ON current_mrr.ultimate_parent_account_id = future_mrr.ultimate_parent_account_id
        AND current_mrr.retention_month = future_mrr.mrr_month

), final AS (

    SELECT
      retention_subs.ultimate_parent_account_id,
      dim_crm_accounts.crm_account_name,
      retention_mrr,
      coalesce(retention_mrr, 0) AS net_retention_mrr,
      CASE WHEN net_retention_mrr > 0
        THEN least(net_retention_mrr, original_mrr)
        ELSE 0 END               AS gross_retention_mrr,
      retention_month,
      original_mrr_month,
      original_mrr,
      {{ churn_type('original_mrr', 'net_retention_mrr') }}
    FROM retention_subs
    LEFT JOIN dim_crm_accounts
      ON dim_crm_accounts.crm_account_id = retention_subs.ultimate_parent_account_id

)

SELECT *
FROM final