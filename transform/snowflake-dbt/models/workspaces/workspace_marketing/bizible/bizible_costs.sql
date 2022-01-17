WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_costs_source') }}

)

SELECT *
FROM source