WITH seat_links AS (

    SELECT
      order_id,
      zuora_subscription_id                                                 AS order_subscription_id,
      TRIM(zuora_subscription_id)                                           AS dim_subscription_id,
      report_date,
      active_user_count,
      license_user_count,
      max_historical_user_count,
      IFF(ROW_NUMBER() OVER (
            PARTITION BY order_subscription_id
            ORDER BY report_date DESC) = 1,
          TRUE, FALSE)                                                      AS is_last_seat_link_report_per_subscription,
      IFF(ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY report_date DESC) = 1,
          TRUE, FALSE)                                                      AS is_last_seat_link_report_per_order
    FROM {{ ref('customers_db_license_seat_links_source') }}

), customers_orders AS (

    SELECT *
    FROM {{ ref('customers_db_orders_source') }}

), subscriptions AS (

    SELECT *
    FROM {{ ref('prep_subscription') }}
    
), product_details AS (

    SELECT DISTINCT
      product_rate_plan_id,
      dim_product_tier_id
    FROM {{ ref('dim_product_detail') }}
    WHERE product_delivery_type = 'Self-Managed'

), joined AS (

    SELECT
      customers_orders.order_id                                             AS customers_db_order_id,
      seat_links.order_subscription_id,
      {{ get_keyed_nulls('subscriptions.dim_subscription_id') }}            AS dim_subscription_id,
      {{ get_keyed_nulls('subscriptions.dim_subscription_id_original') }}   AS dim_subscription_id_original,
      {{ get_keyed_nulls('subscriptions.dim_subscription_id_previous') }}   AS dim_subscription_id_previous,
      {{ get_keyed_nulls('subscriptions.dim_crm_account_id') }}             AS dim_crm_account_id,
      {{ get_keyed_nulls('subscriptions.dim_billing_account_id') }}         AS dim_billing_account_id,
      {{ get_keyed_nulls('product_details.dim_product_tier_id') }}          AS dim_product_tier_id,
      seat_links.active_user_count                                          AS active_user_count,
      seat_links.license_user_count,
      seat_links.max_historical_user_count                                  AS max_historical_user_count,
      seat_links.report_date,
      seat_links.is_last_seat_link_report_per_subscription,
      seat_links.is_last_seat_link_report_per_order,
      IFF(IFNULL(seat_links.order_subscription_id, '') = subscriptions.dim_subscription_id,
          TRUE, FALSE)                                                      AS is_subscription_in_zuora,
      IFF(product_details.dim_product_tier_id IS NOT NULL, TRUE, FALSE)     AS is_rate_plan_in_zuora,
      IFF(seat_links.active_user_count IS NOT NULL, TRUE, FALSE)            AS is_active_user_count_available
    FROM seat_links 
    INNER JOIN customers_orders
      ON seat_links.order_id = customers_orders.order_id
    LEFT OUTER JOIN subscriptions
      ON seat_links.dim_subscription_id = subscriptions.dim_subscription_id
    LEFT OUTER JOIN product_details
      ON customers_orders.product_rate_plan_id = product_details.product_rate_plan_id
      
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-02-02",
    updated_date="2021-02-16"
) }}