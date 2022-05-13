WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_sync_lead_to_sfdc_source') }}

)

SELECT *
FROM source