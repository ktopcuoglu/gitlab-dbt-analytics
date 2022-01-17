WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_advertisers_source') }}

)

SELECT *
FROM source