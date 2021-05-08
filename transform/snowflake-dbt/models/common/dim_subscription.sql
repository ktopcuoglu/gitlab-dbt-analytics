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
    --ids & keys
    subscription.dim_subscription_id,
    subscription.dim_crm_account_id,
    subscription.dim_billing_account_id,
    subscription.dim_crm_person_id_invoice_owner,
    subscription.dim_crm_opportunity_id,
    subscription.dim_subscription_id_original,
    subscription.dim_subscription_id_previous,
    {{ get_keyed_nulls('prep_amendment.dim_amendment_id') }}       AS dim_amendment_id,

    --info
    subscription.subscription_name,
    subscription.subscription_name_slugify,
    subscription.subscription_status,
    subscription.subscription_version,
    subscription.is_auto_renew,
    subscription.zuora_renewal_subscription_name,
    subscription.zuora_renewal_subscription_name_slugify,
    subscription.current_term,
    subscription.renewal_term,
    subscription.renewal_term_period_type,
    subscription.eoa_starter_bronze_offer_accepted,
    subscription.subscription_start_date,
    subscription.subscription_end_date,
    subscription.subscription_sales_type,
    subscription.subscription_start_month,
    subscription.subscription_end_month,
    subscription_lineage.subscription_lineage,
    subscription_lineage.oldest_subscription_in_cohort,
    subscription_lineage.subscription_cohort_month,
    subscription_lineage.subscription_cohort_quarter,
    subscription_lineage.subscription_cohort_year
  FROM subscription
  LEFT JOIN subscription_lineage
    ON subscription_lineage.dim_subscription_id = subscription.dim_subscription_id
  LEFT JOIN prep_amendment
    ON subscription.dim_amendment_id = prep_amendment.dim_amendment_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@iweeks",
    created_date="2020-12-16",
    updated_date="2021-05-10"
) }}
