WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sales_territory_source') }}

)

SELECT *
FROM source
