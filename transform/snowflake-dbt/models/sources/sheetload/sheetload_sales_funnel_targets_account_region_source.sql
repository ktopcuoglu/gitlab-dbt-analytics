WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_funnel_targets_account_region') }}

), renamed AS (

    SELECT
      "Concat"::VARCHAR                         AS fields_concatenated,
      "Account_Region"::VARCHAR                 AS account_region,
      "KPI_Name"::VARCHAR                       AS kpi_name,
      "Target"::VARCHAR                         AS target,
      "Percent_Curve"::VARCHAR                  AS percent_curve,
      "_UPDATED_AT"::FLOAT                      AS last_updated_at
    FROM source

)

SELECT *
FROM renamed