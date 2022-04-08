WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_email_bounced_source') }}

)

SELECT *
FROM source