WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'opportunity_source') }}

)

SELECT
  "Concat"::VARCHAR                       AS fields_concatenated,
  "KPI_Name"::VARCHAR                     AS kpi_name,
  "Opportunity_Source"::VARCHAR           AS opportunity_source,
  "Target"::VARCHAR                       AS target,
  "Percent_Curve"::VARCHAR                AS percent_curve,
  "_UPDATED_AT"::FLOAT                    AS last_updated_at
FROM source
