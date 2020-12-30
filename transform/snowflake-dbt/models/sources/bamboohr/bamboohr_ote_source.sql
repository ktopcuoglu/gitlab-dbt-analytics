{{ config({
    "materialized": "ephemeral"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'custom_on_target_earnings') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), renamed AS (

    SELECT 
      data_by_row.value['id']::NUMBER                       AS target_earnings_update_id,
      data_by_row.value['employeeId']::NUMBER               AS employee_id,
      data_by_row.value['customDate']::DATE                 AS effective_date,
      data_by_row.value['customAnnualAmountLocal']::VARCHAR AS annual_amount_local,
      data_by_row.value['customAnnualAmountUSD']::VARCHAR   AS annual_amount_usd,
      data_by_row.value['customOTELocal']::VARCHAR          AS ote_local,
      data_by_row.value['customOTEUSD']::VARCHAR            AS ote_usd,
      data_by_row.value['customType']::VARCHAR              AS ote_type,
      data_by_row.value['customVariablePay']::VARCHAR       AS variable_pay
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => true)initial_unnest,
    LATERAL FLATTEN(INPUT => parse_json(initial_unnest.value), OUTER => true)data_by_row

), final AS (

    SELECT 
      target_earnings_update_id,
      employee_id,
      effective_date,
      variable_pay,
      annual_amount_local,
      SPLIT_PART(annual_amount_usd,' ',1)   AS annual_amount_usd_value,
      ote_local,
      SPLIT_PART(ote_usd,' ',1)             AS ote_usd,
      ote_type
    FROM renamed

)

SELECT *,
  LAG(COALESCE(annual_amount_usd_value,0)) OVER (PARTITION BY employee_id ORDER BY target_earnings_update_id)   AS prior_annual_amount_usd,
  annual_amount_usd_value - prior_annual_amount_usd                                                             AS change_in_annual_amount_usd
FROM final
