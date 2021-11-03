{{ config(
    tags=["mnpi_exception"]
) }}

{{ config({
    "materialized": "table"
    })
}}

{{ simple_cte([
  ('map_subscription_lineage', 'map_subscription_lineage'),
  ('prep_subscription', 'prep_subscription'),
  ('fct_usage_ping_payload', 'fct_usage_ping_payload'),
])
}}

, active_subscriptions AS (

    SELECT 
      prep_subscription.*,
      STRTOK_TO_ARRAY(subscription_lineage, ',') AS subscription_lineage_array,
      ARRAY_SLICE(subscription_lineage_array, -2, -1)::VARCHAR  AS latest_subscription_in_lineage
    FROM prep_subscription
    LEFT JOIN map_subscription_lineage
      ON prep_subscription.dim_subscription_id = map_subscription_lineage.dim_subscription_id
    WHERE subscription_status IN ('Active', 'Cancelled')
      AND subscription_start_date < subscription_end_date

), usage_ping_with_license AS (

    SELECT
      dim_usage_ping_id,
      fct_usage_ping_payload.ping_created_at,
      dim_subscription_id
    FROM fct_usage_ping_payload

), map_to_all_subscriptions_in_lineage AS (

    SELECT 
      *,
      f.value AS subscription_in_lineage,
      f.index AS lineage_index
    FROM active_subscriptions,
      LATERAL FLATTEN(INPUT => subscription_lineage_array) f

), join_ping_to_subscriptions AS (
    -- this CTE is finding for a specific usage ping the current subscription_id that are valid 
    -- and in the lineage of the subscription that was linked to the usage ping at creation date
    SELECT 
      dim_usage_ping_id,
      usage_ping_with_license.ping_created_at         AS usage_ping_created_at,
      renewal_subscriptions.subscription_start_date   AS subscription_start_date,
      renewal_subscriptions.subscription_end_date     AS subscription_end_date,
      renewal_subscriptions.subscription_name_slugify AS subscription_name_slugify,
      renewal_subscriptions.dim_subscription_id       AS dim_subscription_id
    FROM usage_ping_with_license
    INNER JOIN prep_subscription
      ON usage_ping_with_license.dim_subscription_id = prep_subscription.dim_subscription_id
    INNER JOIN map_to_all_subscriptions_in_lineage AS active_subscriptions 
      ON active_subscriptions.subscription_name_slugify = prep_subscription.subscription_name_slugify
    LEFT JOIN active_subscriptions AS renewal_subscriptions
      ON active_subscriptions.subscription_in_lineage = renewal_subscriptions.subscription_name_slugify

), first_subscription AS (

    -- decision taken because there is in very little cases 1% of the cases, several active subscriptions in a lineage at on a specific month M
    SELECT DISTINCT 
      dim_usage_ping_id,
      FIRST_VALUE(dim_subscription_id) OVER (
        PARTITION BY dim_usage_ping_id
        ORDER BY subscription_start_date ASC
      ) AS dim_subscription_id
    FROM join_ping_to_subscriptions
    WHERE usage_ping_created_at >= subscription_start_date 
      AND usage_ping_created_at <= subscription_end_date

), unioned AS (

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
      join_ping_to_subscriptions.dim_usage_ping_id,
      FIRST_VALUE(join_ping_to_subscriptions.dim_subscription_id) OVER (
        PARTITION BY join_ping_to_subscriptions.dim_usage_ping_id
        ORDER BY subscription_start_date ASC) AS dim_subscription_id,
      NULL                                    AS other_dim_subscription_id_array,
      'Match between Usage Ping and a expired Subscription' AS match_type
    FROM join_ping_to_subscriptions
    LEFT JOIN first_subscription ON join_ping_to_subscriptions.dim_usage_ping_id = first_subscription.dim_usage_ping_id
    WHERE first_subscription.dim_usage_ping_id IS NULL
      AND join_ping_to_subscriptions.dim_subscription_id IS NOT NULL

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-06-21",
    updated_date="2021-06-21"
) }}
