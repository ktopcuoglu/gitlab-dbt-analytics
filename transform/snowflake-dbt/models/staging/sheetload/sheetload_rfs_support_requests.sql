WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_rfs_support_requests') }}

)

SELECT *
FROM source

