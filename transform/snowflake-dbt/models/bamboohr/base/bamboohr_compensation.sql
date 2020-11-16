{{ config({
    "schema": "ephemeral"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'compensation') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

    SELECT 
      d.value as data_by_row, 
      uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), renamed AS (

    SELECT
      data_by_row['id']::NUMBER                AS compensation_update_id,
      data_by_row['employeeId']::NUMBER        AS employee_id,
      data_by_row['startDate']::DATE           AS effective_date,
      data_by_row['type']::VARCHAR             AS compensation_type,
      data_by_row['reason']::VARCHAR           AS compensation_change_reason,
      data_by_row['paidPer']::VARCHAR          AS pay_rate,   
      data_by_row['rate']['value']::FLOAT      AS compensation_value,
      data_by_row['rate']['currency']::VARCHAR AS compensation_currency,
      uploaded_at 
    FROM intermediate
      
)

SELECT *
FROM renamed
{# WHERE compensation_update_id != 20263 ---incorrectly labeled  #}
