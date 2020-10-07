{# {{ config({
    "materialized": "ephemeral"
    })
}} #}

WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'custom_currency_conversion') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), renamed AS (

    SELECT
      e.value['id']::NUMBER                                 AS target_earnings_update_id,
      e.value['employeeId']::NUMBER                         AS employee_id,
      e.value['customDate']::DATE                           AS effective_date,
      e.value['customType']::VARCHAR                        AS compensation_type,
      e.value['customCurrencyConversionFactor']::DECIMAL    AS currency_conversion_factor,
      e.value['customAnnualAmountLocal']::VARCHAR           AS annual_amount_local,
      e.value['customAnnualAmountUSD']::VARCHAR             AS annual_amount_usd,
      e.value['customOTELocal']::VARCHAR                    AS ote_local,
      e.value['customOTEUSD']::VARCHAR                      AS ote_usd,
      e.value['customType']::VARCHAR                        AS ote_type,
      e.value['customVariablePay']::VARCHAR                 AS variable_pay
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => true) initial_unnest,
    LATERAL FLATTEN(INPUT => parse_json(initial_unnest.value), OUTER => true) e

), final AS (

    SELECT 
      target_earnings_update_id,
      employee_id,
      effective_date,
      variable_pay,
      compensation_type,
      currency_conversion_factor,
      REGEXP_REPLACE(annual_amount_local, '[a-z/-/A-z/#/*]', '')::DECIMAL   AS annual_amount_local_value,
      REGEXP_REPLACE(annual_amount_local, '[0-9/-/#/./*]', '')              AS annual_local_currency_code,
      REGEXP_REPLACE(annual_amount_usd, '[a-z/-/A-z/#/*]', '')::DECIMAL     AS annual_amount_usd_value,
      REGEXP_REPLACE(ote_local, '[a-z/-/A-z/#/*]', '')::DECIMAL             AS ote_local_amount,
      REGEXP_REPLACE(ote_local, '[0-9/-/#/./*]', '')                        AS ote_local_currency_code,
      REGEXP_REPLACE(ote_usd, '[a-z/-/A-z/#/*]', '')::DECIMAL               AS ote_usd,
      ote_type
    FROM renamed

), final AS (

    SELECT *,
      LAG(COALESCE(annual_amount_usd_value,0)) OVER (PARTITION BY employee_id ORDER BY target_earnings_update_id)  AS prior_annual_amount_usd,
      annual_amount_usd_value - prior_annual_amount_usd AS change_in_annual_amount_usd
    FROM final

)

SELECT *
FROM final
