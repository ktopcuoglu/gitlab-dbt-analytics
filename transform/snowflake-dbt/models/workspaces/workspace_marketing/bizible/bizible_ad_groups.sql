WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_ad_groups_source') }}

)

SELECT *
FROM source