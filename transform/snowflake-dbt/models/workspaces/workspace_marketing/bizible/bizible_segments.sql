WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_segments_source') }}

)

SELECT *
FROM source