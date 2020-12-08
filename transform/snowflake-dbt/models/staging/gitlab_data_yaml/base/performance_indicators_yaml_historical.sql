WITH unioned AS (

    {{ dbt_utils.union_relations(
        relations=[
            ref('performance_indicators_cost_source'), 
            ref('performance_indicators_corporate_finance_source'),
            ref('performance_indicators_customer_support_source'),
            ref('performance_indicators_dev_section_source'),              
            ref('performance_indicators_development_department_source'),              
            ref('performance_indicators_enablement_section_source'),          
            ref('performance_indicators_engineering_source'),
            ref('performance_indicators_finance_source'),
            ref('performance_indicators_infrastructure_department_source'),
            ref('performance_indicators_marketing_source'),
            ref('performance_indicators_ops_section_source'),
            ref('performance_indicators_people_success_source'),
            ref('performance_indicators_product_source'),
            ref('performance_indicators_quality_department_source'),
            ref('performance_indicators_recruiting_source'),
            ref('performance_indicators_sales_source'),
            ref('performance_indicators_security_department_source'),
            ref('performance_indicators_ux_department_source')
            ]
    ) }}

), intermediate AS (

    SELECT
      pi_name,
      org_name,
      pi_definition,
      is_key,
      is_public,
      is_embedded,
      pi_target,
      telemetry_type,
      pi_url,
      sisense_chart_id,
      sisense_dashboard_id,
      FIRST_VALUE(snapshot_date) OVER (PARTITION BY pi_name ORDER BY snapshot_date) AS date_first_added, 
      snapshot_date AS valid_from_date
    FROM unioned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY pi_name, org_name, pi_definition, is_key, is_public, is_embedded, pi_target, pi_url ORDER BY snapshot_date) =1 

), final AS (

    SELECT *,
    COALESCE(LEAD(valid_from_date) OVER (PARTITION BY pi_name, org_name ORDER BY valid_from_date), CURRENT_DATE()) AS valid_to_date
    FROM intermediate
    
)

SELECT *
FROM final
