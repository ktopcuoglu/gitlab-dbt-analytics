{{ config(
    tags=["product"]
) }}

WITH product_tier AS (

    SELECT *
    FROM {{ ref('prep_product_tier') }}
    WHERE product_delivery_type = 'Self-Managed'

), subscription AS (

    SELECT *
    FROM {{ ref('prep_subscription') }}
    WHERE subscription_end_date >= CURRENT_DATE

), recurring_charge AS (

    SELECT *
    FROM {{ ref('prep_recurring_charge') }}
    WHERE dim_date_id = TO_NUMBER(REPLACE(TO_CHAR(DATE_TRUNC('month', CURRENT_DATE)), '-', '')) --first day of current month date id

), product_detail AS (

    SELECT *
    FROM {{ ref('dim_product_detail') }}
    WHERE product_delivery_type = 'Self-Managed'

), orders AS (

    SELECT *
    FROM {{ ref('customers_db_orders_source') }}

), current_recurring AS (

    --find only Self-Managed recurring charge subscriptions.
    SELECT DISTINCT
      recurring_charge.dim_subscription_id,
      product_detail.product_tier_name,
      product_detail.dim_product_tier_id,
      product_detail.product_rate_plan_id
    FROM recurring_charge
    --new prep table will be required AND new key in prep_recurring_charge
    INNER JOIN product_detail
      ON recurring_charge.dim_product_detail_id = product_detail.dim_product_detail_id
    WHERE LOWER(product_detail.product_name) NOT LIKE '%true%'

), active_subscription_list AS (
  
    --Active Self-Managed Subscriptions, waiting on prep_recurring_revenue
    SELECT
      subscription.dim_subscription_id,  
      current_recurring.product_tier_name                           AS product_tier_name_subscription,
      current_recurring.dim_product_tier_id                         AS dim_product_tier_id_subscription,
      current_recurring.product_rate_plan_id                        AS product_rate_plan_id_subscription,
      subscription.dim_subscription_id_original,
      subscription.dim_subscription_id_previous,
      subscription.subscription_name,
      subscription.subscription_name_slugify,
      subscription.dim_billing_account_id,
      subscription.dim_crm_account_id,
      COUNT(*) OVER (PARTITION BY subscription.dim_subscription_id) AS count_of_tiers_per_subscription
    FROM subscription
    INNER JOIN current_recurring 
      ON subscription.dim_subscription_id = current_recurring.dim_subscription_id
  
), product_rate_plan AS (
  
    SELECT DISTINCT
      product_rate_plan_id,
      dim_product_tier_id,
      product_tier_name,
      product_delivery_type
    FROM product_detail
  
), trial_tier AS ( 
  
    SELECT
      dim_product_tier_id,
      product_tier_name,
      product_delivery_type
    FROM product_tier
    WHERE product_tier_name = 'Self-Managed - Trial: Ultimate'

), active_orders_list AS (

    SELECT
      orders.order_id,
      orders.customer_id, 
      COALESCE(trial_tier.dim_product_tier_id,
               product_rate_plan.dim_product_tier_id)               AS dim_product_tier_id_with_trial,
      COALESCE(trial_tier.product_delivery_type,
               product_rate_plan.product_delivery_type)             AS product_delivery_type_with_trial, 
      COALESCE(trial_tier.product_tier_name,
               product_rate_plan.product_tier_name)                 AS product_tier_name_with_trial, 
      product_rate_plan.dim_product_tier_id                         AS dim_product_tier_id_order,
      product_rate_plan.product_rate_plan_id                        AS product_rate_plan_id_order,
      product_rate_plan.product_delivery_type                       AS product_delivery_type_order, 
      product_rate_plan.product_tier_name                           AS product_tier_name_order, 
      orders.subscription_id                                        AS dim_subscription_id, 
      orders.subscription_name,
      orders.subscription_name_slugify,
      orders.order_start_date, 
      orders.order_end_date,
      orders.order_is_trial
    FROM orders
    INNER JOIN product_rate_plan
      ON orders.product_rate_plan_id = product_rate_plan.product_rate_plan_id
    LEFT JOIN trial_tier 
      ON orders.order_is_trial = TRUE
    WHERE orders.order_end_date >= CURRENT_DATE
      OR orders.order_end_date IS NULL
        
), final AS (

    SELECT
      active_subscription_list.dim_subscription_id, 
      active_orders_list.order_id, 
      active_orders_list.dim_subscription_id                        AS subscription_id_order, 
      active_orders_list.customer_id, 
      active_orders_list.dim_product_tier_id_with_trial,
      active_orders_list.product_delivery_type_with_trial, 
      active_orders_list.product_tier_name_with_trial, 
      active_orders_list.dim_product_tier_id_order,
      active_orders_list.product_delivery_type_order, 
      active_orders_list.product_tier_name_order, 
      active_orders_list.order_start_date, 
      active_orders_list.order_end_date, 
      active_orders_list.order_is_trial,
      active_orders_list.product_rate_plan_id_order,
      active_subscription_list.subscription_name,
      active_subscription_list.product_tier_name_subscription,
      active_subscription_list.dim_product_tier_id_subscription,
      active_subscription_list.dim_subscription_id_original,
      active_subscription_list.dim_subscription_id_previous,
      active_subscription_list.dim_billing_account_id,
      active_subscription_list.dim_crm_account_id,
      active_subscription_list.count_of_tiers_per_subscription,
      active_subscription_list.product_rate_plan_id_subscription
    FROM active_orders_list
    FULL OUTER JOIN active_subscription_list
      ON active_orders_list.dim_subscription_id = active_subscription_list.dim_subscription_id
      OR active_orders_list.dim_subscription_id = active_subscription_list.dim_subscription_id_original
      OR active_orders_list.subscription_name = active_subscription_list.subscription_name_slugify
      --joining on name above only works because we are only dealing with currently active subscriptions  
              
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-02-02",
    updated_date="2021-02-02"
) }}
