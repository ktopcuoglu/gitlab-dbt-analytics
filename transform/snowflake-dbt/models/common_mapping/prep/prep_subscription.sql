WITH date_details AS (

    SELECT *
    FROM {{ ref('date_details') }}

), map_merged_crm_account AS (

    SELECT *
    FROM {{ ref('map_merged_crm_account') }}

), zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), zuora_account AS (

    SELECT
      account_id,
      crm_id
    FROM {{ ref('zuora_account_source') }}

), renewal_subscriptions AS (

    SELECT DISTINCT
      sub_1.subscription_name,
      sub_1.zuora_renewal_subscription_name,
      DATE_TRUNC('month',sub_2.subscription_end_date::DATE)                     AS myb_renewal_month,
      RANK() OVER (PARTITION BY sub_1.subscription_name
                   ORDER BY sub_1.zuora_renewal_subscription_name, sub_2.subscription_end_date )
                                                                                AS rank
    FROM zuora_subscription sub_1
    INNER JOIN zuora_subscription sub_2
      ON sub_1.zuora_renewal_subscription_name = sub_2.subscription_name
    WHERE sub_1.zuora_renewal_subscription_name != ''
    QUALIFY rank = 1

), joined AS (

    SELECT
      zuora_subscription.subscription_id                                        AS dim_subscription_id,
      map_merged_crm_account.dim_crm_account_id                                 AS dim_crm_account_id,
      zuora_account.account_id                                                  AS dim_billing_account_id,
      zuora_subscription.invoice_owner_id                                       AS dim_crm_person_id_invoice_owner,
      zuora_subscription.sfdc_opportunity_id                                    AS dim_crm_opportunity_id,
      zuora_subscription.original_id                                            AS dim_subscription_id_original,
      zuora_subscription.previous_subscription_id                               AS dim_subscription_id_previous,
      zuora_subscription.amendment_id                                           AS dim_amendment_id_subscription,
      zuora_subscription.subscription_name,
      zuora_subscription.subscription_name_slugify,
      zuora_subscription.subscription_status,
      zuora_subscription.version                                                AS subscription_version,
      zuora_subscription.auto_renew                                             AS is_auto_renew,
      zuora_subscription.zuora_renewal_subscription_name,
      zuora_subscription.zuora_renewal_subscription_name_slugify,
      CASE
        WHEN zuora_subscription.current_term >= 24 THEN TRUE
        WHEN zuora_subscription.subscription_name IN (SELECT DISTINCT subscription_name FROM renewal_subscriptions) THEN TRUE
        ELSE FALSE
      END                                                                       AS is_myb,
      CASE
        WHEN zuora_subscription.subscription_name IN (SELECT DISTINCT subscription_name FROM renewal_subscriptions) THEN TRUE
        ELSE FALSE
      END                                                                       AS is_myb_with_multi_subs,
      zuora_subscription.current_term,
      zuora_subscription.renewal_term,
      zuora_subscription.renewal_term_period_type,
      zuora_subscription.eoa_starter_bronze_offer_accepted,
      IFF(zuora_subscription.created_by_id = '2c92a0fd55822b4d015593ac264767f2', -- All Self-Service / Web direct subscriptions are identified by that created_by_id
          'Self-Service', 'Sales-Assisted')                                     AS subscription_sales_type,

      --Date Information
      zuora_subscription.subscription_start_date                                AS subscription_start_date,
      zuora_subscription.subscription_end_date                                  AS subscription_end_date,
      DATE_TRUNC('month', zuora_subscription.subscription_start_date::DATE)     AS subscription_start_month,
      DATE_TRUNC('month', zuora_subscription.subscription_end_date::DATE)       AS subscription_end_month,
      date_details.fiscal_year                                                  AS subscription_end_fiscal_year,
      zuora_subscription.term_start_date::DATE                                  AS term_start_date,
      zuora_subscription.term_end_date::DATE                                    AS term_end_date,
      DATE_TRUNC('month', zuora_subscription.term_start_date::DATE)             AS term_start_month,
      DATE_TRUNC('month', zuora_subscription.term_end_date::DATE)               AS term_end_month,
      CASE
        WHEN LOWER(zuora_subscription.subscription_status) = 'active' AND subscription_end_date > CURRENT_DATE
          THEN DATE_TRUNC('month',DATEADD('month', zuora_subscription.current_term, zuora_subscription.subscription_end_date::DATE))
        ELSE NULL
      END                                                                       AS second_active_renewal_month,
      renewal_subscriptions.myb_renewal_month,
      zuora_subscription.created_date::DATE                                     AS subscription_created_date,
      zuora_subscription.updated_date::DATE                                     AS subscription_updated_date
    FROM zuora_subscription
    INNER JOIN zuora_account
      ON zuora_subscription.account_id = zuora_account.account_id
    LEFT JOIN map_merged_crm_account
      ON zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
    LEFT JOIN renewal_subscriptions
      ON zuora_subscription.subscription_name = renewal_subscriptions.subscription_name
    LEFT JOIN date_details
      ON zuora_subscription.subscription_end_date::DATE = date_details.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@iweeks",
    created_date="2021-01-07",
    updated_date="2021-05-10"
) }}
