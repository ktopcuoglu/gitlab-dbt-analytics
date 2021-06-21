WITH performance_obligation_source AS (

    SELECT *
    FROM {{ ref('zuora_revenue_revenue_contract_performance_obligation_source') }}

), final AS (

    SELECT DISTINCT

      -- ids
      performance_obligation_id,

      -- pob details
      performance_obligation_version,
      revenue_contract_performance_obligation_name,

      -- flags
      is_performance_obligation_dependency,
      is_performance_obligation_processed,
      is_performance_obligation_removed,
      is_performance_obligation_multiple_sign,
      is_performance_obligation_removal,
      is_performance_obligation_manual,
      is_performance_obligation_orphan,
      is_performance_obligation_manual_forecast,
      is_performance_obligation_template_dependency

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