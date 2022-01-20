WITH source AS (

    SELECT *
    FROM {{ ref('bizible_crm_events_source') }}

)

SELECT *
FROM source