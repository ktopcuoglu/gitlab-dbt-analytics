WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','gcp_active_cud') }}
)

SELECT *
FROM source