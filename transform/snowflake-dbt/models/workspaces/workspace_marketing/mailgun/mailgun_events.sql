WITH source AS (

    SELECT {{ hash_sensitive_columns('mailgun_events_source') }}
    FROM {{ ref('mailgun_events_source') }}

)

SELECT *
FROM source