{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ simple_cte([
  ('dim_product_detail', 'dim_product_detail'),
  ('prep_usage_ping_payload', 'prep_usage_ping_payload'),
  ('map_usage_ping_active_subscription', 'map_usage_ping_active_subscription'),
  ('prep_recurring_charge', 'prep_recurring_charge'),
])
}}

, dim_date AS (
    
    SELECT *
    FROM {{ ref('dim_date') }}
    WHERE first_day_of_month < CURRENT_DATE

), self_managed_active_subscriptions AS (

    SELECT
      dim_date_id,
      dim_subscription_id,
          
      SUM(mrr)              AS mrr,
      SUM(quantity)         AS quantity
    FROM prep_recurring_charge
    INNER JOIN dim_product_detail
      ON prep_recurring_charge.dim_product_detail_id = dim_product_detail.dim_product_detail_id
      AND product_delivery_type = 'Self-Managed'
    WHERE subscription_status IN ('Active', 'Cancelled')
    {{ dbt_utils.group_by(n=2) }}

), mau AS (

    SELECT *
    FROM {{ ref('prep_usage_data_28_days_flattened') }}
    WHERE metrics_path = 'usage_activity_by_stage_monthly.manage.events'

), transformed AS (

    SELECT
      {{ dbt_utils.surrogate_key(['first_day_of_month', 'self_managed_active_subscriptions.dim_subscription_id']) }}    AS month_subscription_id,
      date_id                                                                                                           AS dim_date_id,
      self_managed_active_subscriptions.dim_subscription_id,
      mrr*12                                                                                                            AS arr,
      quantity,
      MAX(prep_usage_ping_payload.dim_subscription_id) IS NOT NULL                                                       AS has_sent_payloads,
      COUNT(DISTINCT prep_usage_ping_payload.dim_usage_ping_id)                                                          AS monthly_payload_counts,
      COUNT(DISTINCT prep_usage_ping_payload.host_name)                                                                  AS monthly_host_counts,
      MAX(metric_value)                                                                                                 AS umau
    FROM self_managed_active_subscriptions
    INNER JOIN dim_date 
      ON self_managed_active_subscriptions.dim_date_id = dim_date.date_id
    LEFT JOIN map_usage_ping_active_subscription 
      ON self_managed_active_subscriptions.dim_subscription_id = map_usage_ping_active_subscription.dim_subscription_id
    LEFT JOIN prep_usage_ping_payload 
      ON map_usage_ping_active_subscription.dim_usage_ping_id = prep_usage_ping_payload.dim_usage_ping_id AND first_day_of_month = prep_usage_ping_payload.ping_created_at_month
    LEFT JOIN mau 
      ON prep_usage_ping_payload.dim_usage_ping_id = mau.dim_usage_ping_id
    {{ dbt_utils.group_by(n=5) }}

), latest_versions AS (

    SELECT DISTINCT
      date_id                                                                                                           AS dim_date_id,
      self_managed_active_subscriptions.dim_subscription_id,
      FIRST_VALUE(major_minor_version) OVER (
        PARTITION BY first_day_of_month, self_managed_active_subscriptions.dim_subscription_id
        ORDER BY ping_created_at DESC
      ) AS latest_major_minor_version
    FROM self_managed_active_subscriptions
    INNER JOIN dim_date 
      ON self_managed_active_subscriptions.dim_date_id = dim_date.date_id
    LEFT JOIN map_usage_ping_active_subscription 
      ON self_managed_active_subscriptions.dim_subscription_id = map_usage_ping_active_subscription.dim_subscription_id
    LEFT JOIN prep_usage_ping_payload 
      ON map_usage_ping_active_subscription.dim_usage_ping_id = prep_usage_ping_payload.dim_usage_ping_id 
      AND first_day_of_month = prep_usage_ping_payload.ping_created_at_month

), joined AS (

    SELECT
      transformed.*,
      latest_versions.latest_major_minor_version
    FROM transformed
    LEFT JOIN latest_versions
      ON transformed.dim_date_id = latest_versions.dim_date_id
      AND transformed.dim_subscription_id = latest_versions.dim_subscription_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-06-21",
    updated_date="2021-07-22"
) }}
