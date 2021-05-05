{%- set stage_names = dbt_utils.get_column_values(ref('wk_prep_stages_to_include'), 'stage_name', default=[]) -%}

{{ config({
    "materialized": "table"
    })
}}

{{simple_cte([('monthly_usage_data', 'monthly_usage_data')])}}

, smau_only AS (

    SELECT 
      host_id,
      instance_id,
      {{dbt_utils.surrogate_key(['host_id', 'instance_id'])}} AS organization_id,
      ping_id,
      stage_name,
      created_month,
      monthly_metric_value
    FROM monthly_usage_data
    WHERE is_smau = TRUE

), dim_usage_pings AS (

    SELECT *
    FROM {{ ref('dim_usage_pings') }}

)

SELECT 
  smau_only.created_month AS reporting_month,
  smau_only.organization_id,
  'Self-Managed' AS delivery,
  IFF(instance_user_count = 1, 'Individual', 'Group')          AS organization_type,
  dim_usage_pings.product_tier,
  IFF(dim_usage_pings.product_tier <> 'Core', TRUE, FALSE) AS is_paid_product_tier,
  umau_value,
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
{{dbt_utils.group_by(n=7)}}
