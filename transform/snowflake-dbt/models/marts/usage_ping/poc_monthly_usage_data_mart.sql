WITH monthly_usage_data AS (

    SELECT *
    FROM {{ ref('monthly_usage_data') }}

)
, fct_usage_ping_payloads AS (

    SELECT *
    FROM {{ ref('fct_usage_ping_payloads') }}

)

SELECT 
  created_month,
  clean_metrics_name,
  product_tier,
  stage_name,
  SUM(monthly_metric_value) AS monthly_metric_value_sum
FROM monthly_usage_data
GROUP BY 1,2,3,4
