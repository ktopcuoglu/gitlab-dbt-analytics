{{ config(
    tags=["product"]
) }}

WITH namespace AS (

    SELECT *
    FROM {{ ref('prep_namespace') }}

), product_tier AS (

    SELECT *
    FROM {{ ref('prep_product_tier') }}
    WHERE product_delivery_type = 'SaaS'

), subscription AS (

    SELECT *
    FROM {{ ref('prep_subscription') }}
    WHERE subscription_end_date >= CURRENT_DATE
    AND subscription_status in ('Active','Cancelled')

), recurring_charge AS (

    SELECT *
    FROM {{ ref('prep_recurring_charge') }}
    WHERE dim_date_id = TO_NUMBER(REPLACE(TO_CHAR(DATE_TRUNC('month', CURRENT_DATE)), '-', '')) --first day of current month date id

), product_detail AS (

    SELECT *
    FROM {{ ref('dim_product_detail') }}
    WHERE product_delivery_type = 'SaaS'

), orders AS (

    SELECT *
    FROM {{ ref('customers_db_orders_source') }}

), trial_histories AS (

    SELECT *
    FROM {{ ref('customers_db_trial_histories_source') }}

), current_recurring AS (

    --find only SaaS recurring charge subscriptions.
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

), active_namespace_list AS (

    --contains non-free namespaces + prior trial namespaces.
    SELECT
      namespace.dim_namespace_id,
      namespace.namespace_type,
      product_tier.dim_product_tier_id                              AS dim_product_tier_id_namespace,
      product_tier.product_tier_name                                AS product_tier_name_namespace,
      trial_histories.start_date                                    AS saas_trial_start_date,
      trial_histories.expired_on                                    AS saas_trial_expired_on,
      COALESCE(trial_histories.gl_namespace_id IS NOT NULL, FALSE)  AS namespace_was_trial
    FROM namespace
    INNER JOIN product_tier
      ON namespace.dim_product_tier_id = product_tier.dim_product_tier_id
    LEFT JOIN trial_histories
      ON namespace.dim_namespace_id = trial_histories.gl_namespace_id
    WHERE namespace.namespace_is_ultimate_parent = TRUE
      AND (product_tier.product_tier_name != 'SaaS - Free'
           OR namespace_was_trial)


), active_subscription_list AS (
  
    --Active SaaS Subscriptions, waiting on prep_recurring_revenue
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
    WHERE product_tier_historical = 'SaaS - Trial: Gold'

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
      orders.gitlab_namespace_id                                    AS namespace_id,
      namespace.ultimate_parent_namespace_id,
      orders.order_is_trial
    FROM orders
    INNER JOIN product_rate_plan
      ON orders.product_rate_plan_id = product_rate_plan.product_rate_plan_id
    LEFT JOIN namespace
      ON orders.gitlab_namespace_id = namespace.dim_namespace_id
    LEFT JOIN trial_tier
      ON orders.order_is_trial = TRUE
    WHERE orders.order_end_date >= CURRENT_DATE
      OR orders.order_end_date IS NULL

), final AS (

    SELECT
      active_namespace_list.dim_namespace_id,
      active_subscription_list.dim_subscription_id,
      active_orders_list.order_id,
      active_orders_list.namespace_id                               AS namespace_id_order,
      active_orders_list.dim_subscription_id                        AS subscription_id_order,
      active_namespace_list.dim_namespace_id                        AS ultimate_parent_namespace_id,
      active_namespace_list.namespace_type,
      active_namespace_list.dim_product_tier_id_namespace,
      active_namespace_list.product_tier_name_namespace,
      active_namespace_list.saas_trial_start_date,
      active_namespace_list.saas_trial_expired_on,
      active_namespace_list.namespace_was_trial,
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
      active_subscription_list.product_rate_plan_id_subscription,
      CASE
        WHEN active_namespace_list.product_tier_name_namespace = 'SaaS - Free'
          THEN 'N/A Free'
        WHEN active_namespace_list.product_tier_name_namespace = 'SaaS - Trial: Ultimate'
          AND active_orders_list.order_id IS NULL
          THEN 'Trial Namespace Missing Active Order' 
        WHEN active_namespace_list.product_tier_name_namespace IN ('SaaS - Bronze', 'SaaS - Premium', 'SaaS - Ultimate')
          AND active_orders_list.order_id IS NULL
          THEN 'Paid Namespace Missing Active Order' 
        WHEN active_namespace_list.product_tier_name_namespace IN ('SaaS - Bronze', 'SaaS - Premium', 'SaaS - Ultimate')
          AND active_orders_list.dim_subscription_id IS NULL
          THEN 'Paid Namespace Missing Order Subscription' 
        WHEN active_namespace_list.product_tier_name_namespace IN ('SaaS - Bronze', 'SaaS - Premium', 'SaaS - Ultimate')
          AND active_subscription_list.dim_subscription_id IS NULL
          THEN 'Paid Namespace Missing Zuora Subscription' 
        WHEN active_orders_list.dim_subscription_id IS NOT NULL
          AND active_namespace_list.dim_namespace_id IS NULL
          THEN 'Paid Order Missing Namespace ID Assignment'
        WHEN active_orders_list.order_id IS NOT NULL
          AND active_orders_list.namespace_id IS NULL
          THEN 'Free Order Missing Namespace ID Assignment' 
        WHEN active_subscription_list.dim_subscription_id IS NOT NULL
          AND active_orders_list.order_id IS NULL
          THEN 'Paid Subscription Missing Order'
        WHEN active_orders_list.ultimate_parent_namespace_id != active_orders_list.namespace_id
          THEN 'Order Linked to Non-Ultimate Parent Namespace'
        WHEN active_namespace_list.dim_namespace_id IS NOT NULL
          AND active_orders_list.dim_subscription_id IS NOT NULL
          AND active_subscription_list.dim_subscription_id IS NOT NULL
          THEN 'Paid All Matching'
        WHEN active_namespace_list.product_tier_name_namespace = 'SaaS - Trial: Ultimate'
          AND active_orders_list.order_id IS NOT NULL
          THEN 'Trial All Matching'
        WHEN active_orders_list.order_id IS NOT NULL
          AND active_namespace_list.dim_namespace_id IS NULL
          THEN 'Order Namespace Not Found'
      END                                                           AS namespace_order_subscription_match_status
    FROM active_namespace_list
    FULL OUTER JOIN active_orders_list
      ON active_namespace_list.dim_namespace_id = active_orders_list.namespace_id
      OR active_namespace_list.dim_namespace_id = active_orders_list.ultimate_parent_namespace_id
    FULL OUTER JOIN active_subscription_list
      ON active_orders_list.dim_subscription_id = active_subscription_list.dim_subscription_id
      OR active_orders_list.dim_subscription_id = active_subscription_list.dim_subscription_id_original
      OR active_orders_list.subscription_name_slugify = active_subscription_list.subscription_name_slugify
      --joining on name above only works because we are only dealing with currently active subscriptions  

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-01-14",
    updated_date="2021-02-02"
) }}
