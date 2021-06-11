WITH zuora_revenue_waterfall_summary AS (

    SELECT *
    FROM {{source('zuora_revenue','zuora_revenue_waterfall_summary')}}

), renamed AS (

    SELECT 
      as_of_prd_id::VARCHAR             AS as_of_period_id,
      schd_id::VARCHAR                  AS revenue_contract_schedule_id,
      line_id::VARCHAR                  AS revenue_contract_line_id,
      root_line_id::VARCHAR             AS root_line_id,
      prd_id::VARCHAR                   AS period_id,
      post_prd_id::VARCHAR              AS post_period_id,
      sec_atr_val::VARCHAR              AS security_attribute_value,
      book_id::VARCHAR                  AS book_id,
      client_id::VARCHAR                AS client_id,
      acctg_seg::VARCHAR                AS accounting_segment,
      acctg_type_id::VARCHAR            AS accounting_type_id,
      netting_entry_flag::VARCHAR       AS is_netting_entry,
      schd_type_flag::VARCHAR           AS revenue_contract_schedule_type,
      t_at::VARCHAR                     AS transactional_amount,
      f_at::VARCHAR                     AS functional_amount,
      r_at::VARCHAR                     AS reporting_amount,
      crtd_prd_id::VARCHAR              AS waterfall_created_peridd_id,
      crtd_dt::DATE                     AS waterfall_created_date,
      crtd_by::VARCHAR                  AS waterfall_created_by,
      updt_dt::DATE                     AS waterfall_updated_date,
      updt_by::VARCHAR                  AS waterfall_updated_by,
      incr_updt_dt::DATE                AS incremental_update_date,
    FROM 

)

SELECT *
FROM renamed
