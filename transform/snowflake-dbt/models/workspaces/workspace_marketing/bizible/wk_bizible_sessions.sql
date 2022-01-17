WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_sessions_source') }}

)

SELECT *
FROM source