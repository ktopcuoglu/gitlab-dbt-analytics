{{simple_cte([
  ('dim_usage_pings', 'dim_usage_pings'),
  ('dim_license', 'dim_license'),
  ('dim_subscription', 'dim_subscription')
])
}}

, active_subscriptions AS (

    SELECT *
    FROM dim_subscription
    WHERE subscription_status IN ('Active', 'Cancelled')

), usage_ping_with_license AS (

    SELECT
      id AS dim_usage_ping_id,
      COALESCE(license_subscription_id, dim_license.dim_subscription_id) AS license_subscription_id
    FROM dim_usage_pings
    LEFT JOIN dim_license  ON dim_usage_pings.license_md5 = dim_license.license_md5

)

SELECT 
  dim_usage_ping_id, 
  active_subscriptions.dim_subscription_id,
  active_subscriptions.subscription_name_slugify
FROM usage_ping_with_license
LEFT JOIN dim_subscription
  ON license_subscription_id = dim_subscription_id
LEFT JOIN active_subscriptions 
  ON active_subscriptions.subscription_name_slugify = dim_subscription.subscription_name_slugify
WHERE license_subscription_id IS NOT NULL
