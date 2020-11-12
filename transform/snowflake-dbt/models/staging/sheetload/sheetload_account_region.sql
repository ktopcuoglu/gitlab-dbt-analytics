WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_account_region_source') }}

)

SELECT *
FROM source
