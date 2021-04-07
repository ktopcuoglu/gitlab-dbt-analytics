{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('namespaces', 'prep_namespace'),
    ('subscriptions', 'prep_subscription'),
    ('orders', 'customers_db_orders_source'),
    ('product_tiers', 'prep_product_tier'),
    ('product_details', 'dim_product_detail'),
    ('trial_histories', 'customers_db_trial_histories_source'),
    ('subscription_delivery_types', 'bdg_subscription_product_rate_plan')
]) }}

, product_rate_plans AS (

    SELECT DISTINCT
      product_rate_plan_id,
      dim_product_tier_id,
      product_tier_name
    FROM product_details
    WHERE product_delivery_type = 'SaaS'

), saas_subscriptions AS (

    SELECT DISTINCT
      dim_subscription_id,
      product_rate_plan_id
    FROM subscription_delivery_types
    WHERE product_delivery_type = 'SaaS'

), trial_tiers AS (

    SELECT
      dim_product_tier_id,
      product_tier_name
    FROM product_tiers
    WHERE product_tier_name = 'SaaS - Trial: Ultimate'

), current_recurring AS (

    SELECT *
    FROM {{ ref('prep_recurring_charge') }}
    WHERE dim_date_id = {{ get_date_id("DATE_TRUNC('month', CURRENT_DATE)") }}
    
), active_namespace_list AS (

    --contains non-free namespaces + prior trial namespaces.
    SELECT
      namespaces.dim_namespace_id,
      namespaces.namespace_type,
      product_tiers.dim_product_tier_id                             AS dim_product_tier_id_namespace,
      product_tiers.product_tier_name                               AS product_tier_name_namespace,
      trial_histories.start_date                                    AS saas_trial_start_date,
      trial_histories.expired_on                                    AS saas_trial_expired_on,
      COALESCE(trial_histories.gl_namespace_id IS NOT NULL, FALSE)  AS namespace_was_trial
    FROM namespaces
    INNER JOIN product_tiers
      ON product_tiers.product_delivery_type = 'SaaS'
      AND namespaces.dim_product_tier_id = product_tiers.dim_product_tier_id
    LEFT JOIN trial_histories
      ON namespaces.dim_namespace_id = trial_histories.gl_namespace_id
    WHERE (product_tiers.product_tier_name != 'SaaS - Free'
           OR namespace_was_trial)


), active_subscription_list AS (
  
    SELECT
      subscriptions.dim_subscription_id,
      subscriptions.dim_subscription_id_original,
      subscriptions.dim_subscription_id_previous,
      subscriptions.subscription_name,
      subscriptions.subscription_name_slugify,
      subscriptions.dim_billing_account_id,
      subscriptions.dim_crm_account_id,
      subscriptions.subscription_start_date,
      subscriptions.subscription_end_date,
      product_rate_plans.product_rate_plan_id                       AS product_rate_plan_id_subscription,
      product_rate_plans.dim_product_tier_id                        AS dim_product_tier_id_subscription,
      product_rate_plans.product_tier_name                          AS product_tier_name_subscription,
      COUNT(*) OVER(PARTITION BY subscriptions.dim_subscription_id) AS count_of_tiers_per_subscription,
      IFF(current_recurring.dim_subscription_id IS NULL,
             FALSE, TRUE)                                           AS is_subscription_active
    FROM subscriptions
    INNER JOIN saas_subscriptions
      ON subscriptions.dim_subscription_id = saas_subscriptions.dim_subscription_id
    INNER JOIN product_rate_plans
      ON saas_subscriptions.product_rate_plan_id = product_rate_plans.product_rate_plan_id
    LEFT JOIN current_recurring
      ON saas_subscriptions.dim_subscription_id = current_recurring.dim_subscription_id

), active_orders_list AS (

    SELECT
      orders.order_id,
      orders.customer_id,
      COALESCE(trial_tiers.dim_product_tier_id,
               product_rate_plans.dim_product_tier_id)              AS dim_product_tier_id_with_trial,
      COALESCE(trial_tiers.product_tier_name,
               product_rate_plans.product_tier_name)                AS product_tier_name_with_trial,
      product_rate_plans.dim_product_tier_id                        AS dim_product_tier_id_order,
      product_rate_plans.product_rate_plan_id                       AS product_rate_plan_id_order,
      product_rate_plans.product_tier_name                          AS product_tier_name_order,
      orders.subscription_id                                        AS dim_subscription_id,
      orders.subscription_name,
      orders.subscription_name_slugify,
      orders.order_start_date,
      orders.order_end_date,
      orders.gitlab_namespace_id                                    AS namespace_id,
      orders.order_is_trial
    FROM orders
    INNER JOIN product_rate_plans
      ON orders.product_rate_plan_id = product_rate_plans.product_rate_plan_id
    LEFT JOIN namespaces
      ON orders.gitlab_namespace_id = namespaces.dim_namespace_id
    LEFT JOIN trial_tiers
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
      active_orders_list.product_tier_name_with_trial,
      active_orders_list.dim_product_tier_id_order,
      active_orders_list.product_tier_name_order,
      active_orders_list.order_start_date,
      active_orders_list.order_end_date,
      active_orders_list.order_is_trial,
      active_orders_list.product_rate_plan_id_order,
      active_subscription_list.subscription_name,
      active_subscription_list.dim_subscription_id_original,
      active_subscription_list.dim_subscription_id_previous,
      active_subscription_list.dim_billing_account_id,
      active_subscription_list.dim_crm_account_id,
      active_subscription_list.product_rate_plan_id_subscription,
      active_subscription_list.dim_product_tier_id_subscription,
      active_subscription_list.product_tier_name_subscription,
      active_subscription_list.count_of_tiers_per_subscription,
      active_subscription_list.subscription_start_date,
      active_subscription_list.subscription_end_date,
      active_subscription_list.is_subscription_active,
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
        -- WHEN active_namespace_list.dim_namespace_id != active_orders_list.namespace_id
        --   THEN 'Order Linked to Non-Ultimate Parent Namespace'
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
    FULL OUTER JOIN active_subscription_list
      ON active_orders_list.subscription_name_slugify = active_subscription_list.subscription_name_slugify
      OR active_orders_list.dim_subscription_id = active_subscription_list.dim_subscription_id
      OR active_orders_list.dim_subscription_id = active_subscription_list.dim_subscription_id_original
      --joining on name above only works because we are only dealing with currently active subscriptions  

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-01-14",
    updated_date="2021-04-07"
) }}