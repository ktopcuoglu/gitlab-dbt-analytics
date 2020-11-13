WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'purchase_channel') }}

)

SELECT
  "Concat"::VARCHAR                       AS fields_concatenated,
  "KPI_Name"::VARCHAR                     AS kpi_name,
  "Purchase_Channel"::VARCHAR             AS purchase_channel,
  "Target"::VARCHAR                       AS target,
  "Percent_Curve"::VARCHAR                AS percent_curve,
  "_UPDATED_AT"::FLOAT                    AS last_updated_at
FROM source
