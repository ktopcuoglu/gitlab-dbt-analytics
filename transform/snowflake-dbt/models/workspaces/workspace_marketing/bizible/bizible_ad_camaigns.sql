WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_ad_campaigns_source') }}

)

SELECT *
FROM source