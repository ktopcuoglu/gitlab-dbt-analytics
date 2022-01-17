WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_lead_stage_transitions_source') }}

)

SELECT *
FROM source