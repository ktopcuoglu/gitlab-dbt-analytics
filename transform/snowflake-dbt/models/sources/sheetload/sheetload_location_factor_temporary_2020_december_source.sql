WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'location_factor_temporary_2020_december') }}

), renamed AS (

    SELECT 
      employee_number::VARCHAR          AS employee_number,
      location_factor::FLOAT            AS location_factor
    FROM source

)

SELECT *
FROM renamed
