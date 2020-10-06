WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'custom_on_target_earnings') }}
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
      data_by_row['id']::NUMBER                       AS target_earnings_update_id,
      data_by_row['employeeId']::NUMBER               AS employee_id,
      data_by_row['customDate']::DATE                 AS effective_date,
      data_by_row['type']::VARCHAR                    AS compensation_type,
      data_by_row['customAnnualAmountLocal']::VARCHAR AS annual_amount_local,
      data_by_row['customAnnualAmountUSD']::VARCHAR   AS annual_amount_usd,
      data_by_row['customOTELocal']::VARCHAR          AS ote_local,
      data_by_row['customOTEUSD']::VARCHAR            AS ote_usd,
      data_by_row['customType']::VARCHAR              AS ote_type,
      data_by_row['customVariablePay']::VARCHAR       AS variable_pay
    FROM unnest_again

), final AS (

    SELECT 
      target_earnings_update_id,
      employee_id,
      effective_date,
      variable_pay,
      compensation_type,
      SPLIT_PART(annual_amount_local,' ',1) AS annual_amount_local_value,
      SPLIT_PART(annual_amount_local,' ',2) AS annual_amount_local_currency,
      SPLIT_PART(annual_amount_usd,' ',1) AS annual_amount_usd_value,
      SPLIT_PART(annual_amount_usd,' ',2) AS annual_amount_usd_currency,
      ote_local,
      SPLIT_PART(ote_local,' ',1) AS ote_local_amount,
      SPLIT_PART(ote_local,' ',2) AS ote_local_currency_code,
      SPLIT_PART(ote_usd,' ',1)   AS ote_usd,
      ote_type
    FROM renamed

)

SELECT *,
  LAG(COALESCE(annual_amount_usd_value,0)) OVER (PARTITION BY employee_id ORDER BY target_earnings_update_id)   AS prior_annual_amount_usd,
  annual_amount_usd_value - prior_annual_amount_usd AS change_in_annual_amount_usd
FROM final