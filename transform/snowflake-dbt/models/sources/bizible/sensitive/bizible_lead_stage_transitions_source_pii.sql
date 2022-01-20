WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_lead_stage_transitions_source') }}
    FROM {{ ref('bizible_lead_stage_transitions_source') }}

)

SELECT *
FROM source