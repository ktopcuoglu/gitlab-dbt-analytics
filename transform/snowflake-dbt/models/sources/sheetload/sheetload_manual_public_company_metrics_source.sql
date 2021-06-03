WITH source AS (

    SELECT
      month::DATE                     AS month,
      quarter::VARCHAR                AS quarter,
      year::VARCHAR                   AS year,
      metric_name::VARCHAR            AS metric_name,
      amount::NUMBER                  AS amount
    FROM {{ source('sheetload','manual_public_company_metrics') }}

)

SELECT *
FROM source
