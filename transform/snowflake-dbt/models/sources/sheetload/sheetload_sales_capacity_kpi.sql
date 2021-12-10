WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_capacity_kpi') }}

)

SELECT *
FROM source


