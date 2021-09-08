WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','product_group_mappings') }}
),

renamed as (
    SELECT
      group_name::VARCHAR     AS group_name,
      stage_name::VARCHAR     AS stage_name,
      section_name::VARCHAR   AS section_name
    FROM source
)

SELECT *
FROM renamed
