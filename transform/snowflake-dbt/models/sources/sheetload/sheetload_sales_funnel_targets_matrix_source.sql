WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_funnel_targets_matrix') }}

), renamed AS (

    SELECT
      "KPI_Name"::VARCHAR                                   AS kpi_name,
      "Month"::VARCHAR                                      AS month,
      "Sales_Segment"::VARCHAR                              AS sales_segment,
      "Opportunity_Source"::VARCHAR                         AS opportunity_source,
      "Account_Region"::VARCHAR                             AS account_Region,
      "Allocated_Target"::NUMBER                            AS allocated_target,
      "KPI_Total"::NUMBER                                   AS kpi_total,
      TO_TIMESTAMP(TO_NUMERIC("_UPDATED_AT"))::TIMESTAMP    AS last_updated_at
    FROM source

)

SELECT *
FROM renamed
