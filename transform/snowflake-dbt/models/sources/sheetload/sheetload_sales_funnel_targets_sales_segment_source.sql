WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_funnel_targets_sales_segment') }}

), renamed AS (

    SELECT
      "Concat"::VARCHAR                                     AS fields_concatenated,
      "KPI_Name"::VARCHAR                                   AS kpi_name,
      "Sales_Segment"::VARCHAR                              AS sales_segment,
      "Target"::VARCHAR                                     AS target,
      "Percent_Curve"::VARCHAR                              AS percent_curve,
      TO_TIMESTAMP(TO_NUMERIC("_UPDATED_AT"))::TIMESTAMP    AS last_updated_at
    FROM source

)

SELECT *
FROM renamed
