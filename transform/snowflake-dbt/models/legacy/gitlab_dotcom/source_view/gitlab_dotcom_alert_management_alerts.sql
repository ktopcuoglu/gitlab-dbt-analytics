WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_alert_management_alerts_source') }}

)

SELECT *
FROM source
