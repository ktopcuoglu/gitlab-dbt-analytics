WITH seat_links AS (

    SELECT
      order_id,
      TRIM(zuora_subscription_id)                                   AS dim_subscription_id,
      report_date,
      DATE_TRUNC('month', report_date)                              AS snapshot_month,
      active_user_count,
      license_user_count,
      max_historical_user_count
    FROM {{ ref('customers_db_license_seat_links_source') }}
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY
        dim_subscription_id,
        snapshot_month
      ORDER BY report_date DESC
      ) = 1

), customers_orders AS (

    SELECT *
    FROM {{ ref('customers_db_orders_source') }}

), subscriptions AS (

    SELECT *
    FROM {{ ref('dim_subscription') }}
    
), product_details AS (

    SELECT DISTINCT
      product_rate_plan_id,
      dim_product_tier_id
    FROM {{ ref('dim_product_detail') }}
    WHERE product_delivery_type = 'Self-Managed'

), joined AS (

    SELECT
      -- ids & keys
      customers_orders.order_id                                     AS customers_db_order_id,
      COALESCE(subscriptions.dim_subscription_id, MD5(-1))          AS dim_subscription_id,
      COALESCE(subscriptions.dim_subscription_id_original, MD5(-1)) AS dim_subscription_id_original,
      COALESCE(subscriptions.dim_subscription_id_previous, MD5(-1)) AS dim_subscription_id_previous,
      COALESCE(subscriptions.dim_crm_account_id, MD5(-1))           AS dim_crm_account_id,
      COALESCE(subscriptions.dim_billing_account_id, MD5(-1))       AS dim_billing_account_id,
      COALESCE(product_details.dim_product_tier_id, MD5(-1))        AS dim_product_tier_id,
      
      --counts
      COALESCE(seat_links.active_user_count, 0)                     AS active_user_count,
      seat_links.license_user_count,
      COALESCE(seat_links.max_historical_user_count, 0)             AS max_historical_user_count,
      
      --dates,
      seat_links.snapshot_month,
      seat_links.report_date     
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
    created_date="2021-01-11",
    updated_date="2021-01-11"
) }}