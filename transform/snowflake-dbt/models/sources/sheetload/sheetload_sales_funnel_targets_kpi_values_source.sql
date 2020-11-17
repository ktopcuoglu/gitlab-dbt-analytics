WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_funnel_targets_kpi_values') }}

), renamed AS (

    SELECT
      "KPI_Name"::VARCHAR                                   AS kpi_name,
      "Fiscal_Year_Target"::VARCHAR                         AS fiscal_year_target,
      "Additive?"::VARCHAR                                  AS is_additive,
      "Formula_(if_not_additive)"::VARCHAR                  AS formula,
      "Priority"::VARCHAR                                   AS priority,
      TO_TIMESTAMP(TO_NUMERIC("_UPDATED_AT"))::TIMESTAMP    AS last_updated_at
    FROM source

)

SELECT *
FROM renamed