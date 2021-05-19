{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('subscriptions', 'prep_subscription'),
    ('orders', 'customers_db_orders_source'),
    ('product_details', 'dim_product_detail'),
    ('recurring_charges', 'prep_recurring_charge'),
    ('subscription_delivery_types', 'bdg_subscription_product_rate_plan')
]) }}

, trial_tiers AS (

    SELECT
      dim_product_tier_id,
      product_tier_name
    FROM {{ ref('prep_product_tier') }}
    WHERE product_tier_name = 'Self-Managed - Trial: Ultimate'

), product_rate_plans AS (

    SELECT DISTINCT
      product_rate_plan_id,
      dim_product_tier_id,
      product_tier_name
    FROM product_details
    WHERE product_delivery_type = 'Self-Managed'

), sm_subscriptions AS (

    SELECT DISTINCT
      dim_subscription_id,
      product_rate_plan_id,
      product_rate_plan_charge_name
    FROM subscription_delivery_types
    WHERE product_delivery_type = 'Self-Managed'

), current_recurring AS (

    SELECT *
    FROM recurring_charges
    INNER JOIN product_details
      ON recurring_charges.dim_product_detail_id = product_details.dim_product_detail_id
      AND product_details.product_delivery_type = 'Self-Managed'
    WHERE recurring_charges.dim_date_id = {{ get_date_id("DATE_TRUNC('month', CURRENT_DATE)") }}
      AND subscription_status IN ('Active', 'Cancelled')

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
          FALSE, TRUE)                                              AS is_subscription_active
    FROM subscriptions
    INNER JOIN sm_subscriptions
      ON subscriptions.dim_subscription_id = sm_subscriptions.dim_subscription_id
    INNER JOIN product_rate_plans
      ON sm_subscriptions.product_rate_plan_id = product_rate_plans.product_rate_plan_id
    LEFT JOIN current_recurring
      ON sm_subscriptions.dim_subscription_id = current_recurring.dim_subscription_id

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
      orders.order_is_trial
    FROM orders
    INNER JOIN product_rate_plans
      ON orders.product_rate_plan_id = product_rate_plans.product_rate_plan_id
    LEFT JOIN trial_tiers
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
      active_subscription_list.is_subscription_active
    FROM active_orders_list
    FULL OUTER JOIN active_subscription_list
      ON active_orders_list.subscription_name = active_subscription_list.subscription_name_slugify
      OR active_orders_list.dim_subscription_id = active_subscription_list.dim_subscription_id
      OR active_orders_list.dim_subscription_id = active_subscription_list.dim_subscription_id_original
      --joining on name above only works because we are only dealing with currently active subscriptions

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@iweeks",
    created_date="2021-02-02",
    updated_date="2021-04-28"
) }}
