{%- set stage_names = dbt_utils.get_column_values(ref('wk_prep_stages_to_include'), 'stage_name') -%}


WITH smau_only AS (

    SELECT 
      host_id,
      instance_id,
      ping_id,
      stage_name,
      created_month,
      monthly_metric_value
    FROM {{ ref('monthly_usage_data') }}
    WHERE is_smau

), dim_usage_pings AS (

    SELECT *
    FROM {{ ref('dim_usage_pings') }}

)

SELECT 
  created_month AS reporting_month,
  smau_only.host_id,
  smau_only.instance_id,
  'Self-Managed' AS delivery,
  dim_usage_pings.product_tier,
  {{ dbt_utils.pivot(
    'stage_name', 
    stage_names,
    agg = 'MAX',
    then_value = 'monthly_metric_value',
    else_value = 'NULL',
    suffix='_stage',
    quote_identifiers = False
  ) }}
FROM smau_only
LEFT JOIN dim_usage_pings
  ON smau_only.ping_id = dim_usage_pings.id 
GROUP BY 1,2,3,4,5
