WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','product_group_mappings') }}
)

SELECT *
FROM source
