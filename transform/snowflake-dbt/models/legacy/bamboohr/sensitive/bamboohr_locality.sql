WITH source AS (

    SELECT *
    FROM {{ ref ('bamboohr_id_employee_number_mapping_source_test') }}
    WHERE uploaded_at >= '2020-03-24'
    --1st time we started capturing locality

), intermediate AS (

    SELECT 
      employee_number,
      employee_id,
      locality,
      DATE_TRUNC(day, uploaded_at)                                    AS updated_at
    FROM source
    WHERE locality IS NOT NULL

)
 
SELECT *
FROM intermediate

