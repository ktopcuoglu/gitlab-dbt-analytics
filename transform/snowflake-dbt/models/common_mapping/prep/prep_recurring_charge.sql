/* grain: one record per subscription per month */
WITH dim_date AS (

    SELECT *
    FROM {{ ref('dim_date') }}

), map_merged_crm_account AS (

    SELECT *
    FROM {{ ref('map_merged_crm_account') }}

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
  WHERE charge_type = 'Recurring'

), zuora_subscription AS (

  SELECT *
  FROM {{ ref('zuora_subscription_source') }}
  WHERE is_deleted = FALSE
    AND exclude_from_analysis IN ('False', '')
    AND subscription_status NOT IN ('Draft')

), rate_plan_charge_filtered AS (

  SELECT
    zuora_account.account_id                            AS billing_account_id,
    map_merged_crm_account.dim_crm_account_id           AS crm_account_id,
    zuora_rate_plan_charge.rate_plan_charge_id,
    zuora_subscription.subscription_id,
    zuora_subscription.subscription_name,
    zuora_subscription.subscription_status,
    zuora_rate_plan_charge.product_rate_plan_charge_id  AS product_details_id,
    zuora_rate_plan_charge.mrr,
    zuora_rate_plan_charge.delta_tcv,
    zuora_rate_plan_charge.unit_of_measure,
    zuora_rate_plan_charge.quantity,
    zuora_rate_plan_charge.effective_start_month,
    zuora_rate_plan_charge.effective_end_month
  FROM zuora_rate_plan_charge
  INNER JOIN zuora_rate_plan
    ON zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id
  INNER JOIN zuora_subscription
    ON zuora_rate_plan.subscription_id = zuora_subscription.subscription_id
  INNER JOIN zuora_account
    ON zuora_account.account_id = zuora_subscription.account_id
  LEFT JOIN map_merged_crm_account
    ON zuora_account.crm_id = map_merged_crm_account.sfdc_account_id

), mrr_month_by_month AS (

  SELECT
    dim_date.date_id,
    billing_account_id,
    crm_account_id,
    subscription_id,
    subscription_name,
    subscription_status,
    product_details_id,
    rate_plan_charge_id,
    SUM(mrr)                                             AS mrr,
    SUM(mrr)* 12                                         AS arr,
    SUM(quantity)                                        AS quantity,
    ARRAY_AGG(rate_plan_charge_filtered.unit_of_measure) AS unit_of_measure
  FROM rate_plan_charge_filtered
  INNER JOIN dim_date
    ON rate_plan_charge_filtered.effective_start_month <= dim_date.date_actual
    AND (rate_plan_charge_filtered.effective_end_month > dim_date.date_actual
      OR rate_plan_charge_filtered.effective_end_month IS NULL)
    AND dim_date.day_of_month = 1
  {{ dbt_utils.group_by(n=8) }}

), final AS (

  SELECT
    {{ dbt_utils.surrogate_key(['date_id','rate_plan_charge_id']) }}                        AS mrr_id,
    date_id                                                                                 AS dim_date_id,
    billing_account_id                                                                      AS dim_billing_account_id,
    crm_account_id                                                                          AS dim_crm_account_id,
    subscription_id                                                                         AS dim_subscription_id,
    product_details_id                                                                      AS dim_product_detail_id,
    rate_plan_charge_id                                                                     AS dim_charge_id,
    subscription_status,
    mrr,
    arr,
    quantity,
    unit_of_measure
  FROM mrr_month_by_month

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@iweeks",
    created_date="2021-01-04",
    updated_date="2021-04-28",
) }}
