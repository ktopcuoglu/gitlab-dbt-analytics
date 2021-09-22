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
    --Exclude Batch20 which are the test accounts. This method replaces the manual dbt seed exclusion file.
      AND LOWER(batch) != 'batch20'

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

), active_zuora_subscription AS (

    SELECT *
    FROM zuora_subscription
    WHERE subscription_status IN ('Active', 'Cancelled')

), manual_arr_true_up_allocation AS (

    SELECT *
    FROM {{ ref('sheetload_manual_arr_true_up_allocation_source')}}

), sfdc_opportunity AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_source') }}
    WHERE is_deleted = FALSE

), non_manual_charges AS (

    SELECT
      --Natural Key
      zuora_subscription.subscription_name,
      zuora_subscription.subscription_name_slugify,
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
        WHEN sfdc_opportunity.opportunity_id IS NULL
          THEN NULL
        WHEN sfdc_opportunity.payment_schedule = 'Prepaid'
          THEN TRUE 
        ELSE FALSE
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
        WHEN LOWER(subscription_status) = 'active' AND zuora_subscription.subscription_end_date <= CURRENT_DATE AND is_last_segment = TRUE
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
        WHEN LOWER(subscription_status) = 'active' AND zuora_subscription.subscription_end_date <= CURRENT_DATE AND is_last_segment = TRUE
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
    LEFT JOIN sfdc_opportunity
      ON zuora_subscription.sfdc_opportunity_id = sfdc_opportunity.opportunity_id
        AND zuora_subscription.created_date >= '2021-04-01' -- reliable data is only available from April 2021 forward for opportunty ids on the subscription object

), manual_charges AS ( -- added as a work around until there is an automated method for adding true-up adjustments to Zuora Revenue/Zuora Billing

    SELECT
      active_zuora_subscription.subscription_name                                 AS subscription_name,
      active_zuora_subscription.subscription_name_slugify                         AS subscription_name_slugify,
      active_zuora_subscription.version                                           AS subscription_version,
      NULL                                                                        AS rate_plan_charge_number,
      NULL                                                                        AS rate_plan_charge_version,
      NULL                                                                        AS rate_plan_charge_segment,
      MD5(manual_arr_true_up_allocation.rate_plan_charge_id)                      AS dim_charge_id,
      manual_arr_true_up_allocation.dim_product_detail_id                         AS dim_product_detail_id,
      NULL                                                                        AS dim_amendment_id_charge,
      active_zuora_subscription.subscription_id                                   AS dim_subscription_id,
      manual_arr_true_up_allocation.account_id                                    AS dim_billing_account_id,
      zuora_account.crm_id                                                        AS dim_crm_account_id,
      sfdc_account.ultimate_parent_account_id                                     AS dim_parent_crm_account_id,
      {{ get_date_id('manual_arr_true_up_allocation.effective_start_date') }}     AS effective_start_date_id,
      {{ get_date_id('manual_arr_true_up_allocation.effective_end_date') }}       AS effective_end_date_id,
      active_zuora_subscription.subscription_status                               AS subscription_status,
      'manual true up allocation'                                                 AS rate_plan_name,
      'manual true up allocation'                                                 AS rate_plan_charge_name,
      'TRUE'                                                                      AS is_last_segment,
      NULL                                                                        AS discount_level,
      'Recurring'                                                                 AS charge_type,
      NULL                                                                        AS rate_plan_charge_amendement_type,
      manual_arr_true_up_allocation.unit_of_measure                               AS unit_of_measure,
      'TRUE'                                                                      AS is_paid_in_full,
      active_zuora_subscription.current_term                                      AS months_of_future_billings,
      CASE
        WHEN DATE_TRUNC('month', effective_end_date) > DATE_TRUNC('month', effective_start_date) OR DATE_TRUNC('month', effective_end_date) IS NULL
          THEN TRUE
        ELSE FALSE
      END                                                                         AS is_included_in_arr_calc,
      active_zuora_subscription.subscription_end_date                             AS subscription_end_date,
      effective_start_date                                                        AS effective_start_date,
      effective_end_date                                                          AS effective_end_date,
      DATE_TRUNC('month', effective_start_date)                                   AS effective_start_month,
      DATE_TRUNC('month', effective_end_date)                                     AS effective_end_month,
      effective_end_date                                                          AS charged_through_date,
      accounting_period                                                           AS charge_created_date,
      accounting_period                                                           AS charge_updated_date,
      DATEDIFF('month', effective_start_month::DATE, effective_end_month::DATE)   AS charge_term,
      manual_arr_true_up_allocation.mrr                                           AS mrr,
      NULL                                                                        AS previous_mrr_calc,
      NULL                                                                        AS previous_mrr,
      NULL                                                                        AS delta_mrr_calc,
      NULL                                                                        AS delta_mrr,
      NULL                                                                        AS delta_mrc,
      manual_arr_true_up_allocation.mrr * 12                                      AS arr,
      NULL                                                                        AS previous_arr,
      NULL                                                                        AS delta_arc,
      NULL                                                                        AS delta_arr,
      0                                                                           AS quantity,
      NULL                                                                        AS previous_quantity_calc,
      NULL                                                                        AS previous_quantity,
      NULL                                                                        AS delta_quantity_calc,
      NULL                                                                        AS delta_quantity,
      NULL                                                                        AS tcv,
      NULL                                                                        AS delta_tcv,
      CASE
        WHEN is_paid_in_full = FALSE THEN months_of_future_billings * manual_arr_true_up_allocation.mrr
        ELSE 0
      END                                                                         AS estimated_total_future_billings
    FROM manual_arr_true_up_allocation
    INNER JOIN active_zuora_subscription
      ON manual_arr_true_up_allocation.subscription_name = active_zuora_subscription.subscription_name
    INNER JOIN zuora_account
      ON active_zuora_subscription.account_id = zuora_account.account_id
    LEFT JOIN map_merged_crm_account
      ON zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
    LEFT JOIN sfdc_account
      ON map_merged_crm_account.dim_crm_account_id = sfdc_account.account_id
    LEFT JOIN ultimate_parent_account
      ON sfdc_account.ultimate_parent_account_id = ultimate_parent_account.account_id


), combined_charges AS (

    SELECT *
    FROM non_manual_charges

    UNION

    SELECT *
    FROM manual_charges

), arr_analysis_framework AS (

    SELECT
      combined_charges.*,
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
    FROM combined_charges

)

{{ dbt_audit(
    cte_ref="arr_analysis_framework",
    created_by="@iweeks",
    updated_by="@mcooper",
    created_date="2021-04-28",
    updated_date="2021-09-22"
) }}
