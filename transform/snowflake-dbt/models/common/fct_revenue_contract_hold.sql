WITH hold_source AS (

    SELECT *
    FROM {{ ref('zuora_revenue_revenue_contract_hold_source') }}

), final AS (

    SELECT
      
      -- ids
      revenue_contract_hold_id                      AS dim_revenue_contract_hold_id,
      revenue_contract_id                           AS dim_revenue_contract_id,
      revenue_contract_line_id                      AS dim_revenue_contract_line_id,
      revenue_contract_performance_obligation_id    AS dim_revenue_contract_performance_obligation_id,
      event_id                                      AS dim_accounting_event_id,

      -- accounting segment
      revenue_contract_hold_accounting_segment,

      -- dates
      revenue_hold_start_date,
      revenue_hold_end_date,
      revenue_contract_hold_release_date,
      revenue_contract_hold_expiration_date,
      
      -- flags
      is_revenue_schedule_hold,
      is_revenue_recognition_hold,
      is_allocation_schedule_hold,
      is_allocation_recognition_hold,
      is_user_releasable,
      is_criteria_match,
      is_remove_hold,
      is_manual_hold,
      is_manual_event_hold_applied,
      is_override_approval,
      is_sha_enabled,
      is_allow_manual_apply,
      is_allow_manual_rel,
      is_line_hold_processed,
      

      -- metadata
      revenue_contract_hold_created_by,
      revenue_contract_hold_created_date,
      revenue_contract_hold_updated_by,
      revenue_contract_hold_updated_date,
      incremental_update_date,
      security_attribute_value

    FROM hold_source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2021-06-21",
    updated_date="2021-06-21",
    ) 
 }}