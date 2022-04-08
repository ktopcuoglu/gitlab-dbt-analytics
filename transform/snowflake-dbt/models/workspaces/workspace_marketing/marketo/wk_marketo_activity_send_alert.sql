WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_send_alert_source') }}

)

SELECT *
FROM source