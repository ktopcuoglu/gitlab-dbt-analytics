WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_unsubscribe_email_source') }}

)

SELECT *
FROM source
