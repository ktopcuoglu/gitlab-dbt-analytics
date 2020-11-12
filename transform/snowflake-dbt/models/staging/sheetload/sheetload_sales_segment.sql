WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sales_segment_source') }}

)

SELECT *
FROM source
