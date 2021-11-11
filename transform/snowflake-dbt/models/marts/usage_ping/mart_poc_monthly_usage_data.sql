{{ config(
    tags=["mnpi_exception"]
) }}

WITH fct_monthly_usage_data AS (

    SELECT *
    FROM {{ ref('monthly_usage_data') }}

)
, prep_usage_ping_payload AS (

    SELECT *
    FROM {{ ref('prep_usage_ping_payload') }}

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
      is_paid_gmau,
      is_umau,
      MAX(monthly_metric_value) AS monthly_metric_value
    FROM fct_monthly_usage_data
    WHERE clean_metrics_name IS NOT NULL
    {{ dbt_utils.group_by(n=11) }}
    
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
  is_paid_gmau,
  is_umau,
  usage_ping_delivery_type,
  SUM(monthly_metric_value) AS monthly_metric_value_sum
FROM monthly_usage_data_agg
INNER JOIN prep_usage_ping_payload
  ON monthly_usage_data_agg.ping_id = prep_usage_ping_payload.dim_usage_ping_id
{{ dbt_utils.group_by(n=12) }}
