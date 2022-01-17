WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_keywords_source') }}

)

SELECT *
FROM source