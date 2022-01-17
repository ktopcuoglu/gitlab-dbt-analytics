WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_urls_source') }}

)

SELECT *
FROM source