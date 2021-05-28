WITH retention AS (

    SELECT
      retention_id                AS fct_retention_id,
      dim_crm_account_id          AS dim_crm_account_id,
      dim_subscription_id         AS dim_subscription_id,
      retention_type              AS retention_type,
      arr_segmentation            AS arr_segmentation,
      churn_type                  AS churn_type,
      gross_retention_mrr         AS gross_retention_mrr,
      net_retention_mrr           AS net_retention_mrr,
      original_mrr                AS original_mrr,
      retention_month             AS retention_month,
      months_since_cohort_start   AS months_since_cohort_start,
      cohort_month                AS cohort_month,
      cohort_quarter              AS cohort_quarter,
      quarters_since_cohort_start AS quarters_since_cohort_start
    FROM {{ ref('prep_retention') }}

)

{{ dbt_audit(
    cte_ref="retention",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2021-05-24",
    updated_date="2021-05-24"
) }}
