WITH subscription AS (

    SELECT *
    FROM {{ ref('prep_subscription') }}

), final AS (

  SELECT
    --ids & keys
    dim_subscription_id,
    dim_crm_account_id,
    dim_billing_account_id,
    dim_crm_person_id_invoice_owner,
    dim_crm_opportunity_id,
    dim_subscription_id_original,
    dim_subscription_id_previous,
    amendment_id,

    --info
    subscription_name,
    subscription_name_slugify,
    subscription_status,
    subscription_version,
    is_auto_renew,
    zuora_renewal_subscription_name,
    zuora_renewal_subscription_name_slugify,
    renewal_term,
    renewal_term_period_type,
    subscription_start_date,
    subscription_end_date,
    subscription_sales_type,
    subscription_start_month,
    subscription_end_month
  FROM subscription

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@ischweickartDD",
    created_date="2020-12-16",
    updated_date="2021-01-07"
) }}
