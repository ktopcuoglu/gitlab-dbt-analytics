{{ config({
    "schema": "legacy"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_audit_events_source') }}

)

SELECT *
FROM source
