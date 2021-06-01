{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('prep_namespaces', 'prep_namespace'),
    ('subscriptions', 'prep_subscription'),
    ('orders', 'customers_db_orders_source'),
    ('product_tiers', 'prep_product_tier'),
    ('product_details', 'dim_product_detail'),
    ('recurring_charges', 'prep_recurring_charge'),
    ('trial_histories', 'customers_db_trial_histories_source'),
    ('namespace_lineage', 'gitlab_dotcom_namespace_lineage_prep'),
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
      product_rate_plan_id,
      dim_product_tier_id
    FROM subscription_delivery_types
    WHERE product_delivery_type = 'SaaS'

), trial_tiers AS (

    SELECT
      dim_product_tier_id,
      product_tier_name
    FROM product_tiers
    WHERE product_tier_name = 'SaaS - Trial: Ultimate'

), current_recurring AS (

    SELECT DISTINCT
      recurring_charges.dim_subscription_id,
      product_details.product_rate_plan_id,
      product_details.dim_product_tier_id
    FROM recurring_charges
    INNER JOIN product_details
      ON recurring_charges.dim_product_detail_id = product_details.dim_product_detail_id
      AND product_details.product_delivery_type = 'SaaS'
    WHERE recurring_charges.dim_date_id = {{ get_date_id("DATE_TRUNC('month', CURRENT_DATE)") }}
      AND subscription_status IN ('Active', 'Cancelled')

), namespaces AS (
  
    /*
    This intermediate CTE adds a column to count the distinct plans a namespace has ever been associated with,
    which allows us to filter free namespaces out of the following CTE.
    In this case, "free" is defined as never having a paid plan over the entire history of the given namespace
    */
    SELECT
      *,
      COUNT(DISTINCT IFNULL(gitlab_plan_id, 34))
        OVER (PARTITION BY dim_namespace_id)                            AS gitlab_plan_count
    FROM prep_namespaces

), namespace_list AS (

    --contains non-free namespaces + prior trial namespaces.
    SELECT DISTINCT
      namespaces.dim_namespace_id,
      namespaces.namespace_type,
      namespaces.ultimate_parent_namespace_id,
      namespaces.gitlab_plan_id,
      product_tiers.dim_product_tier_id                                 AS dim_product_tier_id_namespace,
      product_tiers.product_tier_name                                   AS product_tier_name_namespace,
      trial_histories.start_date                                        AS saas_trial_start_date,
      trial_histories.expired_on                                        AS saas_trial_expired_on,
      IFF(trial_histories.gl_namespace_id IS NOT NULL
            OR (namespaces.dim_namespace_id = ultimate_parent_namespace_id
                AND product_tier_name_namespace = 'SaaS - Trial: Ultimate'),
          TRUE, FALSE)                                                  AS namespace_was_trial,
      namespaces.is_currently_valid                                     AS is_namespace_active
    FROM namespaces
    LEFT JOIN product_tiers
      ON namespaces.dim_product_tier_id = product_tiers.dim_product_tier_id
    LEFT JOIN trial_histories
      ON namespaces.dim_namespace_id = trial_histories.gl_namespace_id
    WHERE IFNULL(gitlab_plan_id, 34) != 34                               -- Filter out namespaces that have never had a paid plan
      OR gitlab_plan_count > 1
      OR namespace_was_trial

), subscription_list AS (
  
    SELECT DISTINCT
      subscriptions.dim_subscription_id,
      subscriptions.dim_subscription_id_original,
      subscriptions.dim_subscription_id_previous,
      subscriptions.subscription_name,
      subscriptions.subscription_name_slugify,
      subscriptions.dim_billing_account_id,
      subscriptions.dim_crm_account_id,
      subscriptions.subscription_start_date,
      subscriptions.subscription_end_date,
      product_rate_plans.product_rate_plan_id                           AS product_rate_plan_id_subscription,
      product_rate_plans.dim_product_tier_id                            AS dim_product_tier_id_subscription,
      product_rate_plans.product_tier_name                              AS product_tier_name_subscription,
      COUNT(*) OVER(PARTITION BY subscriptions.dim_subscription_id)     AS count_of_tiers_per_subscription,
      IFF(current_recurring.dim_subscription_id IS NOT NULL,
          TRUE, FALSE)                                                  AS is_subscription_active
    FROM subscriptions
    INNER JOIN saas_subscriptions
      ON subscriptions.dim_subscription_id = saas_subscriptions.dim_subscription_id
    INNER JOIN product_rate_plans
      ON saas_subscriptions.product_rate_plan_id = product_rate_plans.product_rate_plan_id
    LEFT JOIN current_recurring
      ON saas_subscriptions.dim_subscription_id = current_recurring.dim_subscription_id

), order_list AS (

    SELECT
      orders.order_id,
      orders.customer_id,
      COALESCE(trial_tiers.dim_product_tier_id,
               product_rate_plans.dim_product_tier_id)                  AS dim_product_tier_id_with_trial,
      COALESCE(trial_tiers.product_tier_name,
               product_rate_plans.product_tier_name)                    AS product_tier_name_with_trial,
      product_rate_plans.dim_product_tier_id                            AS dim_product_tier_id_order,
      product_rate_plans.product_rate_plan_id                           AS product_rate_plan_id_order,
      product_rate_plans.product_tier_name                              AS product_tier_name_order,
      orders.subscription_id                                            AS subscription_id_order,
      orders.subscription_name                                          AS subscription_name_order,
      orders.subscription_name_slugify                                  AS subscription_name_slugify_order,
      orders.order_start_date,
      orders.order_end_date,
      orders.gitlab_namespace_id                                        AS namespace_id_order,
      orders.order_is_trial,
      IFF(IFNULL(orders.order_end_date, CURRENT_DATE) >= CURRENT_DATE,
          TRUE, FALSE)                                                  AS is_order_active
    FROM orders
    INNER JOIN product_rate_plans
      ON orders.product_rate_plan_id = product_rate_plans.product_rate_plan_id
    LEFT JOIN trial_tiers
      ON orders.order_is_trial = TRUE
    WHERE orders.order_start_date IS NOT NULL 

), final AS (

    SELECT
      namespace_list.dim_namespace_id,
      subscription_list.dim_subscription_id,
      order_list.order_id,
      order_list.namespace_id_order,
      order_list.subscription_id_order,
      namespace_list.ultimate_parent_namespace_id,
      namespace_list.namespace_type,
      namespace_list.dim_product_tier_id_namespace,
      namespace_list.product_tier_name_namespace,
      namespace_list.is_namespace_active,
      namespace_list.namespace_was_trial,
      namespace_list.saas_trial_start_date,
      namespace_list.saas_trial_expired_on,
      order_list.customer_id,
      order_list.product_rate_plan_id_order,
      order_list.dim_product_tier_id_order,
      order_list.product_tier_name_order,
      order_list.is_order_active,
      order_list.order_start_date,
      order_list.order_end_date,
      order_list.order_is_trial,
      order_list.dim_product_tier_id_with_trial,
      order_list.product_tier_name_with_trial,
      subscription_list.subscription_name,
      subscription_list.subscription_name_slugify,
      subscription_list.dim_subscription_id_original,
      subscription_list.dim_subscription_id_previous,
      subscription_list.dim_billing_account_id,
      subscription_list.dim_crm_account_id,
      subscription_list.is_subscription_active,
      subscription_list.subscription_start_date,
      subscription_list.subscription_end_date,
      subscription_list.product_rate_plan_id_subscription,
      subscription_list.dim_product_tier_id_subscription,
      subscription_list.product_tier_name_subscription,
      subscription_list.count_of_tiers_per_subscription,
      CASE
        WHEN namespace_list.gitlab_plan_id IN (102, 103)
          AND order_list.order_id IS NULL
          THEN 'Trial Namespace Missing Order' 
        WHEN order_list.namespace_id_order != namespace_list.ultimate_parent_namespace_id
          AND namespace_list.is_namespace_active = TRUE
          THEN 'Order Linked to Non-Ultimate Parent Namespace'
        WHEN namespace_list.gitlab_plan_id NOT IN (102, 103)
          AND order_list.order_id IS NULL
          THEN 'Paid Namespace Missing Order' 
        WHEN namespace_list.gitlab_plan_id NOT IN (102, 103)
          AND order_list.subscription_id_order IS NULL
          THEN 'Paid Namespace Missing Order Subscription' 
        WHEN namespace_list.gitlab_plan_id NOT IN (102, 103)
          AND subscription_list.dim_subscription_id IS NULL
          THEN 'Paid Namespace Missing Zuora Subscription' 
        WHEN order_list.subscription_id_order IS NOT NULL
          AND namespace_list.dim_namespace_id IS NULL
          THEN 'Paid Order Missing Namespace Assignment'
        WHEN order_list.subscription_id_order IS NOT NULL
          AND order_list.product_rate_plan_id_order IS NOT NULL
          AND subscription_list.dim_subscription_id IS NULL
          THEN 'Paid Order Product Rate Plan Misaligned with Zuora'
        WHEN order_list.order_id IS NOT NULL
          AND order_list.namespace_id_order IS NULL
          THEN 'Free Order Missing Namespace Assignment' 
        WHEN order_list.namespace_id_order IS NOT NULL
          AND namespace_list.dim_namespace_id IS NULL
          THEN 'Order Namespace Not Found'
        WHEN subscription_list.dim_subscription_id IS NOT NULL
          AND order_list.order_id IS NULL
          THEN 'Paid Subscription Missing Order'
        WHEN subscription_list.dim_subscription_id IS NOT NULL
          AND namespace_list.dim_namespace_id IS NOT NULL
          THEN 'Paid All Matching'
        WHEN namespace_list.gitlab_plan_id IN (102, 103)
          AND order_list.order_id IS NOT NULL
          THEN 'Trial All Matching'
      END                                                               AS namespace_order_subscription_match_status
    FROM order_list
    FULL OUTER JOIN subscription_list
      ON order_list.subscription_id_order = subscription_list.dim_subscription_id
      AND order_list.product_rate_plan_id_order = subscription_list.product_rate_plan_id_subscription 
    FULL OUTER JOIN namespace_list
      ON order_list.namespace_id_order = namespace_list.dim_namespace_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-01-14",
    updated_date="2021-06-01"
) }}