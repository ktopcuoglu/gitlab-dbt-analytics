WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','hp_dvr') }}
)

SELECT *
FROM source
