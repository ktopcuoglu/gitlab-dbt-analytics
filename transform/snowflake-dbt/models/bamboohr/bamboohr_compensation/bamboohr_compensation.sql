{{ config({
    "schema": "sensitive"})
}}

WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'compensation') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

      SELECT d.value as data_by_row
      FROM source,
      LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), renamed AS (

      SELECT DISTINCT
            data_by_row['id']::NUMBER          AS compensation_update_id,
            data_by_row['employeeId']::NUMBER  AS employee_id,
            data_by_row['startDate']::date     AS effective_date,
            data_by_row['type']::varchar       AS compensation_type,
            data_by_row['reason']::varchar     AS compensation_change_reason,
            data_by_row['paidPer']::varchar    AS pay_rate,
            data_by_row['rate']                AS rate,
            data_by_row:rate:currency          AS currency,
            data_by_row:rate:value             AS compensation_value,
            uploaded_at 
      FROM intermediate,
      lateral flatten( input => data_by_Row:rate ) a
      FROM intermediate

)

SELECT *
FROM renamed
