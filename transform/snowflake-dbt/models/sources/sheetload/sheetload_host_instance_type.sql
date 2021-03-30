WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'host_usage_type') }}

)

SELECT *
FROM source

