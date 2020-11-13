WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_funnel_target_dates') }}

)

SELECT
  "Concat"::VARCHAR                       AS fields_concatenated,
  "KPI_Name"::VARCHAR                     AS kpi_name,
  "Date"::DATE                            AS sales_funnel_target_date,
  "Target"::VARCHAR                       AS target,
  "Percent_Curve"::VARCHAR                AS percent_curve,
  _UPDATED_AT::FLOAT                      AS last_updated_at
FROM source
