  {{ dbt_utils.union_relations(
      relations=[
              ref('performance_indicators_cost_source'), 
              ref('performance_indicators_corporate_finance_source'),
              ref('performance_indicators_dev_section_source'),              
              ref('performance_indicators_enablement_section_source'),          
              ref('performance_indicators_engineering_source'),
              ref('performance_indicators_finance_source'),
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


