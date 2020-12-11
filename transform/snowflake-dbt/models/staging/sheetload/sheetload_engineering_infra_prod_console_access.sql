WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_engineering_contributing_organizations_source') }}

)

SELECT *
FROM source

