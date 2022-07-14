WITH source AS (

    SELECT *
    FROM {{ source('driveload', 'lam_corrections') }}

)

SELECT *
FROM source
