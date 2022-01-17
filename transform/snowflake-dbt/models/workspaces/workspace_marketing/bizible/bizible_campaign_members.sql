WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_campaign_members_source') }}

)

SELECT *
FROM source