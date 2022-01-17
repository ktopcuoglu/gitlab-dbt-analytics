WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_activities_source') }}

)

SELECT *
FROM source