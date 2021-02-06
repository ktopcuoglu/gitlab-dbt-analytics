WITH subscriptions AS (
    
    SELECT *
    FROM {{ ref('zuora_subscription_source') }}

), rate_plans AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_source') }}

), product_details AS (
    SELECT *
    FROM {{ ref('dim_product_detail') }}

), joined AS (
    SELECT DISTINCT
      subscriptions.subscription_id                     AS dim_subscription_id,
      subscriptions.original_id                         AS dim_subscription_id_original,
      subscriptions.subscription_name,
      subscriptions.subscription_name_slugify,
      subscriptions.subscription_status,
      subscriptions.subscription_start_date,
      subscriptions.subscription_end_date,
      subscriptions.cancelled_date,
      subscriptions.term_end_date,
      subscriptions.term_start_date,
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
    updated_date="2021-02-08"
) }}