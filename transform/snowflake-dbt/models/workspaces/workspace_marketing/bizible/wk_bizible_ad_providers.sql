WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_ad_providers_source') }}

)

SELECT *
FROM source