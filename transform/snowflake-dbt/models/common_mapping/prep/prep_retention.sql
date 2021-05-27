WITH sfdc_account AS (

    SELECT
        arr_segmentation                                as arr_segmentation,
        churn_type                                      as churn_type,
        gross_retention_mrr                             as gross_retention_mrr,
        net_retention_mrr                               as net_retention_mrr,
        original_mrr                                    as original_mrr,
        retention_month                                 as retention_month,
        salesforce_account_id                           as dim_crm_account_id,
        months_since_sfdc_account_cohort_start          as months_since_cohort_start,
        sfdc_account_cohort_month                       as cohort_month,
        sfdc_account_cohort_quarter                     as cohort_quarter,
        ''                                              as dim_subscription_id,
        quarters_since_sfdc_account_cohort_start        as quarters_since_cohort_start,
        'crm_account'                                   as retention_type
    FROM {{ ref ('retention_sfdc_account_') }}

), zuora_subscription AS (

    SELECT
        arr_segmentation                                as arr_segmentation,
        churn_type                                      as churn_type,
        gross_retention_mrr                             as gross_retention_mrr,
        net_retention_mrr                               as net_retention_mrr,
        original_mrr                                    as original_mrr,
        retention_month                                 as retention_month,
        salesforce_account_id                           as dim_crm_account_id,
        months_since_zuora_subscription_cohort_start    as months_since_cohort_start,
        zuora_subscription_cohort_month                 as cohort_month,
        zuora_subscription_cohort_quarter               as cohort_quarter,
        zuora_subscription_id                           as dim_subscription_id,
        quarters_since_zuora_subscription_cohort_start  as quarters_since_cohort_start,
        'dim_subscription'                              as retention_type
    FROM {{ ref('retention_zuora_subscription_') }}

), parent_account AS (

    SELECT
        arr_segmentation                                as arr_segmentation,
        churn_type                                      as churn_type,
        gross_retention_mrr                             as gross_retention_mrr,
        net_retention_mrr                               as net_retention_mrr,
        original_mrr                                    as original_mrr,
        retention_month                                 as retention_month,
        salesforce_account_id                           as dim_crm_account_id,
        months_since_parent_account_cohort_start        as months_since_cohort_start,
        parent_account_cohort_month                     as cohort_month,
        parent_account_cohort_quarter                   as cohort_quarter,
        ''                                              as dim_subscription_id,
        quarters_since_parent_account_cohort_start      as quarters_since_cohort_start,
        'parent_crm_account'                            as retention_type
    FROM {{ ref('retention_parent_account_') }}

), final AS (

SELECT * FROM parent_account
UNION
SELECT * FROM sfdc_account
UNION
SELECT * FROM zuora_subscription
)




{{ dbt_audit(
    cte_ref="final",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2021-05-22",
    updated_date="2021-05-22"
) }}