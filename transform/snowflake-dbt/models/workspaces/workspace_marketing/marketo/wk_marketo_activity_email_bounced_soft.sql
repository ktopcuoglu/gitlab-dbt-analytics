WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_email_bounced_soft_source') }}

)

SELECT *
FROM source