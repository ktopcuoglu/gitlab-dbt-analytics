WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_training_completion_dates') }}

)

SELECT
  Concat::VARCHAR                         AS concat,
  Account_Region::VARCHAR                 AS account_region,
  KPI_Name::VARCHAR                       AS kpi_name,
  Target::VARCHAR                         AS target,
  Percent_Curve::VARCHAR                  AS percent_curve,
  _updated_at::FLOAT                      AS last_updated_at
FROM source
