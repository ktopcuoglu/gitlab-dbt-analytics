WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_currencies_source') }}

)

SELECT *
FROM source