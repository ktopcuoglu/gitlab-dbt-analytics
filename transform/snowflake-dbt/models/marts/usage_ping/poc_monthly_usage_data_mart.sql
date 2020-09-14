WITH monthly_usage_data AS (

    SELECT *
    FROM {{ ref('monthly_usage_data') }}

)

SELECT 
  created_month,
  clean_metrics_name,
  SUM(monthly_metric_value) AS monthly_metric_value_sum
FROM monthly_usage_data
GROUP BY 1,2
