WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','engineering_speciality_prior_to_capture') }}

) 

SELECT * 
FROM source
