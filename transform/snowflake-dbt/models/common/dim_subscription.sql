WITH prep_amendment AS (

  SELECT *
  FROM {{ ref('prep_amendment') }}

), subscription AS (

    SELECT *
    FROM {{ ref('prep_subscription') }}

), subscription_lineage AS (

    SELECT *
    FROM {{ ref('map_subscription_lineage') }}

), final AS (

  SELECT
    --Surrogate Key
    subscription.dim_subscription_id,

    --Natural Key
    subscription.subscription_name,
    subscription.subscription_version,

    --Common Dimension Keys
    subscription.dim_crm_account_id,
    subscription.dim_billing_account_id,
    subscription.dim_crm_person_id_invoice_owner,
    subscription.dim_crm_opportunity_id,
    {{ get_keyed_nulls('prep_amendment.dim_amendment_id') }}       AS dim_amendment_id_subscription,

    --Subscription Information
    subscription.dim_subscription_id_original,
    subscription.dim_subscription_id_previous,
    subscription.subscription_name_slugify,
    subscription.subscription_status,
    subscription.is_auto_renew,
    subscription.zuora_renewal_subscription_name,
    subscription.zuora_renewal_subscription_name_slugify,
    subscription.is_myb,
    subscription.is_myb_with_multi_subs,
    subscription.current_term,
    subscription.renewal_term,
    subscription.renewal_term_period_type,
    subscription.eoa_starter_bronze_offer_accepted,
    subscription.subscription_sales_type,

    --Date Information
    subscription.subscription_start_date,
    subscription.subscription_end_date,
    subscription.subscription_start_month,
    subscription.subscription_end_month,
    subscription.subscription_end_fiscal_year,
    subscription.created_date,
    subscription.myb_renewal_month,
    subscription.term_start_date,
    subscription.term_end_date,
    subscription.term_start_month,
    subscription.term_end_month,
    subscription.second_active_renewal_month,

    --Lineage and Cohort Information
    subscription_lineage.subscription_lineage,
    subscription_lineage.oldest_subscription_in_cohort,
    subscription_lineage.subscription_cohort_month,
    subscription_lineage.subscription_cohort_quarter,
    subscription_lineage.subscription_cohort_year

  FROM subscription
  LEFT JOIN subscription_lineage
    ON subscription_lineage.dim_subscription_id = subscription.dim_subscription_id
  LEFT JOIN prep_amendment
    ON subscription.dim_amendment_id_subscription = prep_amendment.dim_amendment_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@iweeks",
    created_date="2020-12-16",
    updated_date="2021-05-10"
) }}
