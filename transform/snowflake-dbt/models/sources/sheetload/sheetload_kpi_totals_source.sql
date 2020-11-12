WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'kpi_totals') }}

)

SELECT
  "KPI_Name"::VARCHAR                       AS kpi_name,
  "Fiscal_Year_Target"::VARCHAR             AS fiscal_year_target,
  "Additive?"::VARCHAR                    AS is_additive,
  "Formula_(if_not_additive)"::VARCHAR    AS formula,
  "priority"::VARCHAR                       AS priority,
  "_updated_at"::FLOAT                      AS last_updated_at
FROM source
