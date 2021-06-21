WITH performance_obligation_source AS (

    SELECT *
    FROM {{ ref('zuora_revenue_revenue_contract_performance_obligation_source') }}

), final AS (

    SELECT DISTINCT
    
      -- ids
      performance_obligation_template_id,

      -- pob template details
      performance_obligation_template_name,
      performance_obligation_template_description,
      performance_obligation_template_version,
      
      -- meta data
      performance_obligation_template_created_by,
      performance_obligation_template_created_date,
      performance_obligation_template_updated_by,
      performance_obligation_template_updated_date

    FROM performance_obligation_source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2021-06-21",
    updated_date="2021-06-21",
    ) 
 }}