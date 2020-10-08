WITH source AS (

    SELECT *
    FROM {{ source('sheetload','sales_training_completion_dates') }}

)

SELECT
 *
FROM source
