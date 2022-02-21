{%- set stage_names = dbt_utils.get_column_values(ref('wk_prep_stages_to_include'), 'stage_name', default=[]) -%}

{{ config(
    tags=["mnpi_exception"]
) }}

{{ config({
    "materialized": "table"
    })
}}

{{simple_cte([('fct_monthly_usage_data', 'fct_monthly_usage_data')])}}

, smau_only AS (

    SELECT 
      host_name,
      dim_instance_id,
      {{dbt_utils.surrogate_key(['host_name', 'dim_instance_id'])}} AS organization_id,
      dim_usage_ping_id                                             AS dim_usage_ping_id,    
      stage_name,
      ping_created_month,
      monthly_metric_value
    FROM fct_monthly_usage_data
    WHERE is_smau = TRUE

), fct_usage_ping_payload AS (

    SELECT *
    FROM {{ ref('fct_usage_ping_payload') }}

)

SELECT 
  smau_only.ping_created_month                                    AS reporting_month,
  smau_only.organization_id,
  'Self-Managed' AS delivery,
  IFF(instance_user_count = 1, 'Individual', 'Group')             AS organization_type,
  fct_usage_ping_payload.product_tier,
  IFF(fct_usage_ping_payload.product_tier <> 'Core', TRUE, FALSE) AS is_paid_product_tier,
  umau_value,
  COUNT(DISTINCT IFF(monthly_metric_value > 0, stage_name, NULL)) AS active_stage_count,
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
LEFT JOIN fct_usage_ping_payload
  ON smau_only.dim_usage_ping_id = fct_usage_ping_payload.dim_usage_ping_id
{{dbt_utils.group_by(n=7)}}
