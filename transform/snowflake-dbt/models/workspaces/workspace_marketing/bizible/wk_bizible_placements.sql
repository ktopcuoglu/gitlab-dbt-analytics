WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_placements_source') }}

)

SELECT *
FROM source