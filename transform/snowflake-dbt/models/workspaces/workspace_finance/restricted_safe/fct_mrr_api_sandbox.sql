/* grain: one record per rate_plan_charge per month */
WITH mrr AS (

    SELECT
      mrr_id,
      dim_date_id,
      dim_charge_id,
      dim_product_detail_id,
      dim_subscription_id,
      dim_billing_account_id,
      dim_crm_account_id,
      subscription_status,
      mrr,
      arr,
      quantity,
      unit_of_measure
    FROM {{ ref('prep_recurring_charge_api_sandbox') }}
    WHERE mrr != 0 /* This excludes Education customers (charge name EDU or OSS) with free subscriptions */
)

{{ dbt_audit(
    cte_ref="mrr",
    created_by="@ken_aguilar",
    updated_by="@ken_aguilar",
    created_date="2021-09-02",
    updated_date="2021-09-02",
) }}