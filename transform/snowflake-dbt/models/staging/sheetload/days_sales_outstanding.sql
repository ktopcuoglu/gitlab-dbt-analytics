WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_days_sales_outstanding') }}

)

SELECT *
FROM source
