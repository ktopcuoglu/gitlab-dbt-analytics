WITH map_merged_crm_account AS (

    SELECT *
    FROM {{ ref('map_merged_crm_account') }}

), sfdc_account AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE account_id IS NOT NULL

), ultimate_parent_account AS (

    SELECT
      account_id
    FROM sfdc_account
    WHERE account_id = ultimate_parent_account_id

), zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}
    WHERE is_deleted = FALSE

), zuora_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_source') }}

), zuora_rate_plan_charge AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge_source') }}

), zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), recurring_charges AS (

    SELECT
      --Natural Key
      zuora_subscription.subscription_name,
      zuora_subscription.version                                        AS subscription_version,
      zuora_rate_plan_charge.rate_plan_charge_number,
      zuora_rate_plan_charge.version                                    AS rate_plan_charge_version,
      zuora_rate_plan_charge.segment                                    AS rate_plan_charge_segment,

      --Surrogate Key
      zuora_rate_plan_charge.rate_plan_charge_id                        AS dim_charge_id,

      --Common Dimension Keys
      zuora_rate_plan_charge.product_rate_plan_charge_id                AS dim_product_detail_id,
      zuora_rate_plan.amendement_id                                     AS dim_amendment_id_charge,
      zuora_rate_plan.subscription_id                                   AS dim_subscription_id,
      zuora_rate_plan_charge.account_id                                 AS dim_billing_account_id,
      map_merged_crm_account.dim_crm_account_id                         AS dim_crm_account_id,
      ultimate_parent_account.account_id                                AS dim_parent_crm_account_id,
      {{ get_date_id('zuora_rate_plan_charge.effective_start_date') }}  AS effective_start_date_id,
      {{ get_date_id('zuora_rate_plan_charge.effective_end_date') }}    AS effective_end_date_id,

      --Information
      zuora_subscription.subscription_status                            AS subscription_status,
      zuora_rate_plan.rate_plan_name                                    AS rate_plan_name,
      zuora_rate_plan_charge.rate_plan_charge_name,
      zuora_rate_plan_charge.is_last_segment,
      zuora_rate_plan_charge.discount_level,
      zuora_rate_plan_charge.charge_type,
      zuora_rate_plan.amendement_type                                   AS rate_plan_charge_amendement_type,
      zuora_rate_plan_charge.unit_of_measure,
      CASE
        WHEN DATE_TRUNC('month',zuora_rate_plan_charge.charged_through_date) = zuora_rate_plan_charge.effective_end_month::DATE
          THEN TRUE ELSE FALSE
      END                                                               AS is_paid_in_full,
      CASE
        WHEN charged_through_date IS NULL THEN zuora_subscription.current_term
        ELSE DATEDIFF('month',DATE_TRUNC('month', zuora_rate_plan_charge.charged_through_date::DATE), zuora_rate_plan_charge.effective_end_month::DATE)
      END                                                               AS months_of_future_billings,
      CASE
        WHEN effective_end_month > effective_start_month OR effective_end_month IS NULL
          THEN TRUE
        ELSE FALSE
      END                                                               AS is_included_in_arr_calc,

      --Dates
      zuora_subscription.subscription_end_date                          AS subscription_end_date,
      zuora_rate_plan_charge.effective_start_date::DATE                 AS effective_start_date,
      zuora_rate_plan_charge.effective_end_date::DATE                   AS effective_end_date,
      zuora_rate_plan_charge.effective_start_month::DATE                AS effective_start_month,
      zuora_rate_plan_charge.effective_end_month::DATE                  AS effective_end_month,
      zuora_rate_plan_charge.charged_through_date::DATE                 AS charged_through_date,
      zuora_rate_plan_charge.created_date::DATE                         AS charge_created_date,
      zuora_rate_plan_charge.updated_date::DATE                         AS charge_updated_date,
      DATEDIFF(month, zuora_rate_plan_charge.effective_start_month::DATE, zuora_rate_plan_charge.effective_end_month::DATE)
                                                                        AS charge_term,

      --Additive Fields
      zuora_rate_plan_charge.mrr,
      LAG(zuora_rate_plan_charge.mrr,1) OVER (PARTITION BY zuora_subscription.subscription_name, zuora_rate_plan_charge.rate_plan_charge_number
                                              ORDER BY zuora_rate_plan_charge.segment, zuora_subscription.version)
                                                                        AS previous_mrr_calc,
      CASE
        WHEN previous_mrr_calc IS NULL
          THEN 0 ELSE previous_mrr_calc
      END                                                               AS previous_mrr,
      zuora_rate_plan_charge.mrr - previous_mrr                         AS delta_mrr_calc,
      CASE
        WHEN LOWER(subscription_status) = 'active' AND subscription_end_date <= CURRENT_DATE AND is_last_segment = TRUE
          THEN -previous_mrr
        WHEN LOWER(subscription_status) = 'cancelled' AND is_last_segment = TRUE
          THEN -previous_mrr
        ELSE delta_mrr_calc
      END                                                               AS delta_mrr,
      zuora_rate_plan_charge.delta_mrc,
      zuora_rate_plan_charge.mrr * 12                                   AS arr,
      previous_mrr * 12                                                 AS previous_arr,
      zuora_rate_plan_charge.delta_mrc * 12                             AS delta_arc,
      delta_mrr * 12                                                    AS delta_arr,
      zuora_rate_plan_charge.quantity,
      LAG(zuora_rate_plan_charge.quantity,1) OVER (PARTITION BY zuora_subscription.subscription_name, zuora_rate_plan_charge.rate_plan_charge_number
                                                   ORDER BY zuora_rate_plan_charge.segment, zuora_subscription.version)
                                                                        AS previous_quantity_calc,
      CASE
        WHEN previous_quantity_calc IS NULL
          THEN 0 ELSE previous_quantity_calc
      END                                                               AS previous_quantity,
      zuora_rate_plan_charge.quantity - previous_quantity               AS delta_quantity_calc,
      CASE
        WHEN LOWER(subscription_status) = 'active' AND subscription_end_date <= CURRENT_DATE AND is_last_segment = TRUE
          THEN -previous_quantity
        WHEN LOWER(subscription_status) = 'cancelled' AND is_last_segment = TRUE
          THEN -previous_quantity
        ELSE delta_quantity_calc
      END                                                               AS delta_quantity,
      zuora_rate_plan_charge.tcv,
      zuora_rate_plan_charge.delta_tcv,
      CASE
        WHEN is_paid_in_full = FALSE THEN months_of_future_billings * zuora_rate_plan_charge.mrr
        ELSE 0
      END                                                               AS estimated_total_future_billings

    FROM zuora_rate_plan
    INNER JOIN zuora_rate_plan_charge
      ON zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id
    INNER JOIN zuora_subscription
      ON zuora_rate_plan.subscription_id = zuora_subscription.subscription_id
    INNER JOIN zuora_account
      ON zuora_subscription.account_id = zuora_account.account_id
    LEFT JOIN map_merged_crm_account
      ON zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
    LEFT JOIN sfdc_account
      ON map_merged_crm_account.dim_crm_account_id = sfdc_account.account_id
    LEFT JOIN ultimate_parent_account
      ON sfdc_account.ultimate_parent_account_id = ultimate_parent_account.account_id
    WHERE charge_type = 'Recurring'

), arr_analysis_framework AS (

    SELECT
      recurring_charges.*,
      CASE
        WHEN subscription_version = 1
          THEN 'New'
        WHEN LOWER(subscription_status) = 'active' AND subscription_end_date <= CURRENT_DATE
          THEN 'Churn'
        WHEN LOWER(subscription_status) = 'cancelled'
          THEN 'Churn'
        WHEN arr < previous_arr AND arr > 0
          THEN 'Contraction'
        WHEN arr > previous_arr AND subscription_version > 1
          THEN 'Expansion'
        WHEN arr = previous_arr
          THEN 'No Impact'
        ELSE NULL
      END                 AS type_of_arr_change
    FROM recurring_charges

)

{{ dbt_audit(
    cte_ref="arr_analysis_framework",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-04-28",
    updated_date="2021-05-10"
) }}
