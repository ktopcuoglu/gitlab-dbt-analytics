WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','zuora_golden_records') }}

)

SELECT * 
FROM source
