WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_conversion_rates_source') }}

)

SELECT *
FROM source