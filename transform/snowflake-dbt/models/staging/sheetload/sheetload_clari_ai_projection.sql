WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_clari_ai_projection_source') }}

)

SELECT *
FROM source
