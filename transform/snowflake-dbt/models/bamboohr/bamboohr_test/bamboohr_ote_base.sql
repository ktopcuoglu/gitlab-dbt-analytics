{# {{ config({
    "schema": "sensitive",
    "materialized": "ephemeral"
    })
}} #}


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
      data_by_row['customOTELocal']::VARCHAR          AS ote_local,
      data_by_row['customOTEUSD']::VARCHAR            AS ote_usd,
      data_by_row['customAnnualAmountUSD']::VARCHAR   AS annual_amount_usd,
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
      ote_local,
      SPLIT_PART(ote_local,' ',1) AS ote_local_amount,
      SPLIT_PART(ote_local,' ',2) AS ote_local_currency_code,
      SPLIT_PART(ote_usd,' ',1)   AS ote_usd,
      annual_amount_usd,
      ote_type
    FROM renamed

)

SELECT 
  target_earnings_update_id,
  employee_id,
  effective_date,
  variable_pay,
  ote_local_amount,
  ote_local_currency_code,
  ote_usd,
  LAG(ote_usd) OVER (PARTITION BY employee_id ORDER BY target_earnings_update_id) AS prior_bamboohr_ote
FROM final
