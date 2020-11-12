WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_kpi_totals_source') }}

)

SELECT *
FROM source
