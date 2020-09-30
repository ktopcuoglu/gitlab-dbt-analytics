WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_audit_event_details_source') }}

)

SELECT *
FROM source
