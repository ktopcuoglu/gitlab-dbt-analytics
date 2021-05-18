{{ config({
    "materialized": "table"
    })
}}

{{ simple_cte([
  ('dim_usage_pings', 'dim_usage_pings'),
  ('dim_license', 'dim_license'),
  ('dim_subscription', 'dim_subscription')
])
}}

, active_subscriptions AS (

    SELECT 
      *,
      SPLIT_PART(subscription_lineage, ',', -1)  AS latest_subscription_in_lineage,
      STRTOK_TO_ARRAY(subscription_lineage, ',') AS subscription_lineage_array
    FROM dim_subscription
    WHERE subscription_status IN ('Active', 'Cancelled')
      AND subscription_start_date < subscription_end_date

), usage_ping_with_license AS (

    SELECT
      id AS dim_usage_ping_id,
      license_expire_date,
      dim_usage_pings.created_at,
      COALESCE(license_subscription_id, dim_license.dim_subscription_id) AS license_subscription_id
    FROM dim_usage_pings
    LEFT JOIN dim_license  ON dim_usage_pings.license_md5 = dim_license.license_md5

), map_to_all_subscriptions_in_lineage AS (

    SELECT *,
    f.value AS subscription_in_lineage,
    f.index AS lineage_index
    FROM active_subscriptions,
      LATERAL FLATTEN(INPUT => subscription_lineage_array) f

), join_ping_to_subscriptions AS (

    SELECT 
      dim_usage_ping_id,
      license_subscription_id,
      usage_ping_with_license.created_at              AS usage_ping_created_at,
      renewal_subscriptions.subscription_start_date   AS subscription_start_date,
      renewal_subscriptions.subscription_end_date     AS subscription_end_date,
      renewal_subscriptions.subscription_name_slugify AS subscription_name_slugify,
      renewal_subscriptions.dim_subscription_id       AS dim_subscription_id
    FROM usage_ping_with_license
    LEFT JOIN dim_subscription
      ON license_subscription_id = dim_subscription_id
    INNER JOIN map_to_all_subscriptions_in_lineage AS active_subscriptions 
      ON active_subscriptions.subscription_name_slugify = dim_subscription.subscription_name_slugify
    LEFT JOIN active_subscriptions AS renewal_subscriptions
      ON active_subscriptions.subscription_in_lineage = renewal_subscriptions.subscription_name_slugify

), first_subscription AS (

  SELECT DISTINCT 
    dim_usage_ping_id,
    FIRST_VALUE(dim_subscription_id) OVER (
      PARTITION BY dim_usage_ping_id
      ORDER BY subscription_start_date ASC
    ) AS dim_subscription_id
  FROM join_ping_to_subscriptions
  WHERE usage_ping_created_at >= subscription_start_date 
    AND usage_ping_created_at <= subscription_end_date

)

-- FIRST CTE: valid subscriptions when the usage ping got created
SELECT
  join_ping_to_subscriptions.dim_usage_ping_id,
  first_subscription.dim_subscription_id,
  ARRAY_AGG(join_ping_to_subscriptions.dim_subscription_id) WITHIN GROUP (
    ORDER BY subscription_start_date ASC) AS other_dim_subscription_id_array,
  'Match between Usage Ping and Active Subscription' AS match_type
FROM join_ping_to_subscriptions
LEFT JOIN first_subscription
  ON join_ping_to_subscriptions.dim_usage_ping_id = first_subscription.dim_usage_ping_id
WHERE usage_ping_created_at >= subscription_start_date 
  AND usage_ping_created_at <= subscription_end_date
GROUP BY 1,2

UNION 

-- SECOND CTE: No valid subscriptions at usage ping creation
SELECT DISTINCT
  dim_usage_ping_id,
  license_subscription_id AS dim_subscription_id,
  NULL                    AS other_dim_subscription_id_array,
  'Match between Usage Ping and a expired Subscription'
FROM join_ping_to_subscriptions
WHERE dim_usage_ping_id NOT IN (SELECT dim_usage_ping_id FROM first_subscription)
