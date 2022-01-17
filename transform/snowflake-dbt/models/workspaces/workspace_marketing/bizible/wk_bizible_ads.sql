WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_ads_source') }}

)

SELECT *
FROM source