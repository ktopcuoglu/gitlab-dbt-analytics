WITH monthly_usage_data AS (

    SELECT *
    FROM {{ ref('monthly_usage_data') }}

)
, fct_usage_ping_payloads AS (

    SELECT *
    FROM {{ ref('fct_usage_ping_payloads') }}

), monthly_usage_data_agg AS (

    SELECT 
      created_month,
      clean_metrics_name,
      instance_id,
      ping_id,
      group_name,
      stage_name,
      section_name, 
      is_smau,
      is_gmau,
      MAX(monthly_metric_value) AS monthly_metric_value
    FROM monthly_usage_data
    GROUP BY 1,2,3,4,5,6,7,8,9
    
)

SELECT 
  created_month,
  clean_metrics_name,
  edition,
  product_tier,
  group_name,
  stage_name,
  section_name, 
  is_smau,
  is_gmau,
  ping_source,
  SUM(monthly_metric_value) AS monthly_metric_value_sum
FROM monthly_usage_data_agg
INNER JOIN fct_usage_ping_payloads
  ON monthly_usage_data_agg.ping_id = fct_usage_ping_payloads.usage_ping_id
GROUP BY 1,2,3,4,5,6,7,8,9,10
