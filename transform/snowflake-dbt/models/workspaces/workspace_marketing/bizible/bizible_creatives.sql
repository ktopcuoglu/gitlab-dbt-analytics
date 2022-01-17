WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_creatives_source') }}

)

SELECT *
FROM source