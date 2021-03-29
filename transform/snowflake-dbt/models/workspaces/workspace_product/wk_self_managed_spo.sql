{%- set stage_names = dbt_utils.get_column_values(ref('wk_prep_stages_to_include'), 'stage_name') -%}


WITH smau_only AS (

    SELECT 
      host_id,
      instance_id,
      stage_name,
      created_month,
      monthly_metric_value
    FROM {{ ref('monthly_usage_data') }}
    WHERE is_smau

)

SELECT host_id,
   instance_id,
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
  GROUP BY 1,2
