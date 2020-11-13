WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sales_funnel_target_dates_source') }}

)

SELECT *
FROM source
