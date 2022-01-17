WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_stage_definitions_source') }}

)

SELECT *
FROM source