{{ config(
    tags=["mnpi_exception"]
) }}

WITH prep_amendment AS (

  SELECT *
  FROM {{ ref('prep_amendment') }}

), subscription AS (

    SELECT *
    FROM {{ ref('prep_subscription') }}

), subscription_opportunity_mapping AS (

    SELECT *
    FROM {{ ref('map_subscription_opportunity') }}

), subscription_lineage AS (

    SELECT DISTINCT
      subscription_name_slugify,
      subscription_lineage,
      oldest_subscription_in_cohort,
      subscription_cohort_month,
      subscription_cohort_quarter,
      subscription_cohort_year
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
    subscription.dim_billing_account_id_invoice_owner,
    CASE
       WHEN subscription.subscription_created_date < '2019-02-01'
         THEN NULL
       ELSE subscription_opportunity_mapping.dim_crm_opportunity_id
    END                                                                             AS dim_crm_opportunity_id,
    {{ get_keyed_nulls('prep_amendment.dim_amendment_id') }}                        AS dim_amendment_id_subscription,

    --Subscription Information
    subscription.created_by_id,
    subscription.updated_by_id,
    subscription.dim_subscription_id_original,
    subscription.dim_subscription_id_previous,
    subscription.subscription_name_slugify,
    subscription.subscription_status,
    subscription.namespace_id,
    subscription.namespace_name,
    subscription.zuora_renewal_subscription_name,
    subscription.zuora_renewal_subscription_name_slugify,
    subscription.current_term,
    subscription.renewal_term,
    subscription.renewal_term_period_type,
    subscription.eoa_starter_bronze_offer_accepted,
    subscription.subscription_sales_type,
    subscription.auto_renew_native_hist,
    subscription.auto_renew_customerdot_hist,
    subscription.turn_on_cloud_licensing,
    subscription.turn_on_operational_metrics,
    subscription.contract_operational_metrics,
    subscription.contract_auto_renewal,
    subscription.turn_on_auto_renewal,
    subscription.contract_seat_reconciliation,
    subscription.turn_on_seat_reconciliation,
    subscription_opportunity_mapping.is_questionable_opportunity_mapping,

    --Date Information
    subscription.subscription_start_date,
    subscription.subscription_end_date,
    subscription.subscription_start_month,
    subscription.subscription_end_month,
    subscription.subscription_end_fiscal_year,
    subscription.subscription_created_date,
    subscription.subscription_updated_date,
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
    ON subscription_lineage.subscription_name_slugify = subscription.subscription_name_slugify
  LEFT JOIN prep_amendment
    ON subscription.dim_amendment_id_subscription = prep_amendment.dim_amendment_id
  LEFT JOIN subscription_opportunity_mapping
    ON subscription.dim_subscription_id = subscription_opportunity_mapping.dim_subscription_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@michellecooper",
    created_date="2020-12-16",
    updated_date="2021-11-11"
) }}
