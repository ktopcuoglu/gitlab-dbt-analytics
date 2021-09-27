WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_credit') }}

), renamed AS (

    SELECT

      amount,
      amount_display_symbol,
      amount_unit_type_id,
      batch_number,
      business_group_id,
      created_by_id,
      created_by_name,
      created_date,
      credit_id,
      credit_type_id,
      credit_type_name,
      customer_id,
      customer_name,
      eff_participant_id,
      eff_position_id,
      estimated_rel_date,
      ever_on_hold,
      geography_id,
      geography_name,
      incentive_date,
      is_active,
      is_held,
      is_processed,
      is_rollable,
      item_code,
      mgr_eff_part_id,
      mgr_eff_pos_id,
      mgr_master_pos_id,
      modified_by_id,
      modified_by_name,
      modified_date,
      name,
      order_code,
      order_item_id,
      participant_id,
      period_id,
      period_name,
      plan_id,
      plan_name,
      position_id,
      product_id,
      product_name,
      reason_code_id,
      reason_code_name,
      release_date,
      release_group_id,
      rollable_on_reporting,
      rule_id,
      rule_name,
      run_id,
      source_credit_id,
      source_position_id,
      src_pos_relation_type_id,
      sub_batch_number,
      sub_part_key,
      trans_id,
      {{ nohash_sensitive_columns('participant_name', 'position_name') }}

    FROM source
    
)

SELECT *
FROM renamed