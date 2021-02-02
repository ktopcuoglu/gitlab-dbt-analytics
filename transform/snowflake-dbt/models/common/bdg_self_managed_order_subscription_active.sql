{{ config(
    tags=["product"]
) }}

WITH namespace AS (

    SELECT *
    FROM {{ ref('prep_namespace') }}

), product_tier AS (

    SELECT *
    FROM {{ ref('prep_product_tier') }}
    WHERE product_delivery_type = 'Self-Managed'

), subscription AS (

    SELECT *
    FROM {{ ref('prep_subscription') }}
    WHERE subscription_status = 'Active'

), recurring_charge AS (

    SELECT *
    FROM {{ ref('prep_recurring_charge') }}

), product_detail AS (

    SELECT *
    FROM {{ ref('dim_product_detail') }}
    WHERE product_delivery_type = 'Self-Managed'

), orders AS (

    SELECT *
    FROM {{ ref('customers_db_orders_source') }}

), trial_histories AS (

    SELECT *
    FROM {{ ref('customers_db_trial_histories_source') }}

), active_subscription_list AS (
  
    --Active Self-Managed Subscriptions, waiting on prep_recurring_revenue
    SELECT
      subscription.dim_subscription_id,  
      sm_subscriptions.product_tier_name                          AS product_tier_name_subscription,
      sm_subscriptions.dim_product_tier_id                        AS dim_product_tier_id_subscription,
      sm_subscriptions.product_rate_plan_id                       AS product_rate_plan_id_subscription,
      sm_subscriptions.is_oss_or_edu_rate_plan                  AS is_oss_or_edu_rate_plan_subscription,
      subscription.dim_subscription_id_original,
      subscription.dim_subscription_id_previous,
      subscription.subscription_name,
      subscription.dim_billing_account_id,
      subscription.dim_crm_account_id,
      COUNT(*) OVER (PARTITION BY subscription.dim_subscription_id) AS count_of_tiers_per_subscription
    FROM subscription
    JOIN (
          --find only Self-Managed recurring charge subscriptions.
          SELECT DISTINCT
            recurring_charge.dim_subscription_id,
            product_detail.product_tier_name,
            product_detail.dim_product_tier_id,
            product_rate_plan_id,
            product_detail.is_oss_or_edu_rate_plan
          FROM recurring_charge
          --new prep table will be required AND new key in prep_recurring_charge
          JOIN product_detail
            ON recurring_charge.dim_product_detail_id = product_detail.dim_product_detail_id
          WHERE recurring_charge.dim_date_id = TO_NUMBER(REPLACE(TO_CHAR(DATE_TRUNC('month', CURRENT_DATE)), '-', '')) --first day of current month --get Date ID
            AND LOWER(product_detail.product_name) NOT LIKE '%true%'
         ) sm_subscriptions
      ON subscription.dim_subscription_id = sm_subscriptions.dim_subscription_id
  
), product_rate_plan AS (
  
    SELECT DISTINCT
      product_rate_plan_id,
      dim_product_tier_id,
      product_tier_name,
      product_delivery_type
    FROM product_detail
    WHERE product_delivery_type = 'Self-Managed'
  
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
      orders.order_start_date, 
      orders.order_end_date,
      orders.order_is_trial
    FROM orders
    JOIN product_rate_plan
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
      active_subscription_list.product_tier_name_subscription,
      active_subscription_list.dim_product_tier_id_subscription,
      active_subscription_list.dim_subscription_id_original,
      active_subscription_list.dim_subscription_id_previous,
      active_subscription_list.dim_billing_account_id,
      active_subscription_list.dim_crm_account_id,
      active_subscription_list.count_of_tiers_per_subscription,
      active_subscription_list.product_rate_plan_id_subscription,
      active_subscription_list.is_oss_or_edu_rate_plan_subscription
    FROM active_orders_list
    FULL OUTER JOIN active_subscription_list
      ON active_orders_list.dim_subscription_id = active_subscription_list.dim_subscription_id
      OR active_orders_list.dim_subscription_id = active_subscription_list.dim_subscription_id_original
      OR active_orders_list.subscription_name = active_subscription_list.subscription_name --using name JOIN until slug or other field is available in all tables.   
      --joining on name above only works because we are only dealing with currently active subscriptions  
              
)

SELECT * 
FROM final 
