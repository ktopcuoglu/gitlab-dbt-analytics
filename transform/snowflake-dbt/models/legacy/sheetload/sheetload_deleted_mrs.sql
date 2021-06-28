WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_deleted_mrs') }}

)

SELECT *
FROM source
