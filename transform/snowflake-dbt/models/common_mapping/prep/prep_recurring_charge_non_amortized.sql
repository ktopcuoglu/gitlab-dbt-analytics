WITH zuora_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_source') }}

), zuora_rate_plan_charge AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge_source') }}

), base_charges AS (

    SELECT
      zuora_rate_plan_charge.rate_plan_charge_id                        AS dim_charge_id,
      zuora_rate_plan_charge.product_rate_plan_charge_id                AS dim_product_detail_id,
      zuora_rate_plan.subscription_id                                   AS dim_subscription_id,
      zuora_rate_plan_charge.account_id                                 AS dim_billing_account_id,
      zuora_rate_plan.amendement_id                                     AS dim_amendment_id,
      zuora_rate_plan_charge.rate_plan_charge_number,
      zuora_rate_plan.rate_plan_name                                    AS rate_plan_name,
      zuora_rate_plan_charge.rate_plan_charge_name,
      zuora_rate_plan_charge.effective_start_month,
      zuora_rate_plan_charge.effective_end_month,
      {{ get_date_id('zuora_rate_plan_charge.effective_start_date') }}  AS effective_start_date_id,
      {{ get_date_id('zuora_rate_plan_charge.effective_end_date') }}    AS effective_end_date_id,
      {{ get_date_id('zuora_rate_plan_charge.effective_start_month') }} AS effective_start_month_id,
      {{ get_date_id('zuora_rate_plan_charge.effective_end_month') }}   AS effective_end_month_id,
      zuora_rate_plan_charge.unit_of_measure,
      zuora_rate_plan_charge.quantity,
      zuora_rate_plan_charge.mrr,
      zuora_rate_plan_charge.delta_tcv,
      zuora_rate_plan_charge.delta_mrc,
      zuora_rate_plan_charge.discount_level,
      zuora_rate_plan_charge.segment                                    AS rate_plan_charge_segment,
      zuora_rate_plan_charge.version                                    AS rate_plan_charge_version,
      zuora_rate_plan_charge.charge_type,
      zuora_rate_plan.amendement_type
    FROM zuora_rate_plan
    INNER JOIN zuora_rate_plan_charge
      ON zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id
    WHERE (effective_end_month > effective_start_month OR effective_end_month IS NULL)
      AND charge_type = 'Recurring'

)

{{ dbt_audit(
    cte_ref="base_charges",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-04-28",
    updated_date="2021-04-28"
) }}
