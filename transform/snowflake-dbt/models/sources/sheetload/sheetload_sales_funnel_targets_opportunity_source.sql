WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_funnel_targets_opportunity') }}

), renamed AS (

    SELECT
      "Concat"::VARCHAR                                     AS fields_concatenated,
      "KPI_Name"::VARCHAR                                   AS kpi_name,
      "Opportunity_Source"::VARCHAR                         AS opportunity_source,
      "Target"::VARCHAR                                     AS target,
      "Percent_Curve"::VARCHAR                              AS percent_curve,
      TO_TIMESTAMP(TO_NUMERIC("_UPDATED_AT"))::TIMESTAMP    AS last_updated_at
    FROM source

)

SELECT *
FROM renamed