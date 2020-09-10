WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_ar_aging_details') }}

)

SELECT *
FROM source
