WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_opportunity_source') }}

)

SELECT *
FROM source
