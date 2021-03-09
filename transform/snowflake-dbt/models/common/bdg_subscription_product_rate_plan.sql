{{ simple_cte([
    ('rate_plans','zuora_rate_plan_source'),
    ('product_details','dim_product_detail')
]) }}

, subscriptions AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), joined AS (

    SELECT DISTINCT
      subscriptions.subscription_id                     AS dim_subscription_id,
      subscriptions.original_id                         AS dim_subscription_id_original,
      subscriptions.account_id                          AS dim_billing_account_id,
      subscriptions.subscription_name,
      subscriptions.subscription_name_slugify,
      product_details.dim_product_detail_id,
      product_details.product_rate_plan_id,
      product_details.product_id,
      product_details.dim_product_tier_id,
      product_details.product_rate_plan_charge_name,    
      product_details.product_delivery_type
    FROM subscriptions
    LEFT JOIN rate_plans
      ON rate_plans.subscription_id = subscriptions.subscription_id
    LEFT JOIN product_details
      ON rate_plans.product_rate_plan_id = product_details.product_rate_plan_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-02-08",
    updated_date="2021-02-16"
) }}