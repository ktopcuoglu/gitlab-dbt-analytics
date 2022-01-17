WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_sites_source') }}

)

SELECT *
FROM source