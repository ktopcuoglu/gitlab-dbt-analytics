{{ config({
    "schema": "sensitive",
    "materialized": "ephemeral"
    })
}}


WITH source AS (

    SELECT *
    FROM "RAW"."BAMBOOHR"."CUSTOMONTARGETEARNINGS"
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

      SELECT d.value AS data_by_row
      FROM source,
      LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), unnest_again AS (

      SELECT d.value AS data_by_row
      FROM intermediate,
      LATERAL FLATTEN(INPUT => parse_json(data_by_row), OUTER => true) d  

), renamed AS (

      SELECT 
        data_by_row['id']::NUMBER                       AS custom_target_earnings_update_id,
        data_by_row['employeeId']::NUMBER               AS employee_id,
        data_by_row['customDate']::DATE                 AS effective_date,
        data_by_row['type']::VARCHAR                    AS compensation_type,
        data_by_row['customOTELocal']::VARCHAR          AS custom_ote_local,
        data_by_row['customOTEUSD']::VARCHAR            AS custom_ote_usd,
        data_by_row['customAnnualAmountUSD']::VARCHAR   AS custom_annual_amount_usd,
        data_by_row['customType']::VARCHAR              AS custom_ote_type,
        data_by_row['customVariablePay']::VARCHAR       AS custom_variable_pay
      FROM unnest_again

), final AS (

  SELECT 
    custom_target_earnings_update_id,
    employee_id,
    effective_date,
    compensation_type,
    custom_ote_local,
    SPLIT_PART(custom_ote_usd,' ',1) AS custom_ote_usd,
    custom_annual_amount_usd,
    custom_ote_type,
    custom_variable_pay
  FROM renamed

)

SELECT 
  custom_target_earnings_update_id,
  employee_id,
  effective_date,
  custom_ote_usd,
  LAG(custom_ote_usd) OVER (PARTITION BY employee_id ORDER BY custom_target_earnings_update_id) AS prior_bamboohr_ote
FROM final

