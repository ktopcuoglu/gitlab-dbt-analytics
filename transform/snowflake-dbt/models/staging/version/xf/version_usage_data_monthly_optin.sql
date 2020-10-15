WITH self_managed_active_subscriptions AS (
  
    SELECT
      date_id,
      subscription_id,
      product_details_id,
      mrr,
      quantity
    FROM {{ ref('fct_mrr')}}
  
), dim_dates AS (
  
    SELECT DISTINCT
      date_id,
      first_day_of_month
    FROM {{ ref('dim_dates')}}
    WHERE first_day_of_month < CURRENT_DATE
  
), dim_product_details AS (

  SELECT *
  FROM {{ ref('dim_product_details') }}

), active_subscriptions AS (
  
    SELECT *
    FROM {{ ref('dim_subscriptions') }}
  
), all_subscriptions AS (
  
    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
  
), fct_payloads AS (
  
    SELECT *
    FROM {{ ref('fct_usage_ping_payloads') }}
  
), joined AS (
  
    SELECT 
      first_day_of_month AS arr_month,
      self_managed_active_subscriptions.subscription_id,
      active_subscriptions.subscription_name_slugify,
      active_subscriptions.subscription_start_date,
      active_subscriptions.subscription_end_date,
      SUM(self_managed_active_subscriptions.mrr)          AS mrr,
      SUM(self_managed_active_subscriptions.quantity)     AS quantity,
      MAX(fct_payloads.subscription_id) IS NOT NULL       AS has_sent_payloads,
      COUNT(DISTINCT fct_payloads.subscription_id)        AS monthly_payload_counts,
      COUNT(DISTINCT host_id)                             AS monthly_host_counts
    FROM self_managed_active_subscriptions  
    INNER JOIN dim_product_details
      ON self_managed_active_subscriptions.product_details_id = dim_product_details.product_details_id
        AND delivery='Self-Managed'
    INNER JOIN dim_dates ON self_managed_active_subscriptions.date_id = dim_dates.date_id
    LEFT JOIN active_subscriptions ON self_managed_active_subscriptions.subscription_id = active_subscriptions.subscription_id
    LEFT JOIN all_subscriptions ON active_subscriptions.subscription_name_slugify = all_subscriptions.subscription_name_slugify
    LEFT JOIN fct_payloads ON all_subscriptions.subscription_id = fct_payloads.subscription_id AND first_day_of_month = DATE_TRUNC('month', fct_payloads.created_at)
    GROUP BY 1,2,3,4,5
)

SELECT *
FROM joined
ORDER BY 1 DESC
