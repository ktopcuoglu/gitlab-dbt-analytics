WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_user_touchpoints_source') }}

)

SELECT *
FROM source