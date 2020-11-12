WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'opportunity_source') }}

)

SELECT
  Concat::VARCHAR                         AS concat,
  KPI_Name::VARCHAR                       AS kpi_name,
  Opportunity_Source::VARCHAR             AS opportunity_source,
  Target::VARCHAR                         AS target,
  Percent_Curve::VARCHAR                  AS percent_curve,
  application_exercise_complete::DATE     AS application_exercise_compled_date,
  _updated_at::FLOAT                      AS last_updated_at
FROM source
