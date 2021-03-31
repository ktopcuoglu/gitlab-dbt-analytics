WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'host_instance_type') }}

)

SELECT *
FROM source

