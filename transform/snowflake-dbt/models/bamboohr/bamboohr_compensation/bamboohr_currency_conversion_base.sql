{# {{ config({
    "schema": "sensitive",
    "materialized": "ephemeral"
    })
}} #}

WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'custom_currency_conversion') }}

    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

    SELECT 
      d.value AS data_by_row,
      uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => true) d

), unnest_again AS (

    SELECT 
      d.value AS data_by_row,
      uploaded_at
    FROM intermediate,
    LATERAL FLATTEN(INPUT => parse_json(data_by_row), OUTER => true) d  

), renamed AS (

    SELECT 
      data_by_row['id']::NUMBER                                      AS conversion_id,
      data_by_row['employeeId']::NUMBER                              AS employee_id,
      data_by_row['customConversionEffectiveDate']::DATE             AS effective_date,
      data_by_row['customCurrencyConversionFactor']::VARCHAR         AS currency_conversion_factor,
      data_by_row['customLocalAnnualSalary']::VARCHAR                AS local_annual_salary,
      data_by_row['customUSDAnnualSalary']::VARCHAR                  AS usd_annual_salary,
      uploaded_at
    FROM unnest_again
  
), cleaned AS (

    SELECT 
      renamed.*,
      TO_NUMBER(SPLIT_PART(local_annual_salary,' ',1)) AS local_annual_salary_amount,
      SPLIT_PART(local_annual_salary,' ',2)            AS local_currency_code,  
      TO_NUMBER(SPLIT_PART(usd_annual_salary,' ',1))   AS usd_annual_salary_amount,
      SPLIT_PART(usd_annual_salary,' ',2)              AS usd_currency_code
    FROM renamed


), final AS (

    SELECT 
    conversion_id,
    employee_id,
    effective_date,
    currency_conversion_factor,
    local_annual_salary_amount,
    local_currency_code,
    usd_annual_salary_amount
    FROM cleaned 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, usd_annual_salary_amount ORDER BY conversion_id) = 1

) 

SELECT *,
  LAG(usd_annual_salary_amount) OVER (PARTITION BY employee_id ORDER BY conversion_id) AS prior_bamboohr_annual_salary
FROM final 