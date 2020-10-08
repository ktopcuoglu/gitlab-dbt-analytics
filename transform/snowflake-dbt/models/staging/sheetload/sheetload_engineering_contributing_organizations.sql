WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_engineering_contributing_organizations') }}

)

SELECT *
FROM source
