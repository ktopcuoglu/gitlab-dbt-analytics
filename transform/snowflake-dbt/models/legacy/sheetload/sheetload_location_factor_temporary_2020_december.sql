{{ config({
    "schema": "legacy"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_location_factor_temporary_2020_december_source') }}

)

SELECT *
FROM source
