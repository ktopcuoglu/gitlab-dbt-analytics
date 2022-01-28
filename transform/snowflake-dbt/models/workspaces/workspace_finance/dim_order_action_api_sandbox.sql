WITH base AS (

    SELECT *
    FROM {{ref('zuora_api_sandbox_order_action_source')}}
    WHERE is_deleted = FALSE

), final AS (

    SELECT
      
      id                                 AS dim_order_action_id,
      orderid                            AS dim_order_id,
      subscriptionid                     AS dim_subscription_id,
      subscriptionversionamendmentid     AS dim_amendment_id,
      type                               AS order_action_type,
      sequence                           AS order_action_sequence,
      autorenew                          AS is_auto_renew,
      cancellationpolicy                 AS cancellation_policy,
      termtype                           AS term_type,
      createddate                        AS order_action_created_date,
      customeracceptancedate             AS customer_acceptance_date,
      contracteffectivedate              AS contract_effective_date,
      serviceactivationdate              AS service_activation_date,
      currentterm                        AS current_term,
      currenttermperiodtype              AS current_term_period_type,
      renewalterm                        AS renewal_term,
      renewaltermperiodtype              AS renewal_term_period_type,
      renewsetting                       AS renewal_setting,
      termstartdate                      AS term_start_date

    FROM base

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ken_aguilar",
    updated_by="@ken_aguilar",
    created_date="2022-01-28",
    updated_date="2022-01-28"
) }}