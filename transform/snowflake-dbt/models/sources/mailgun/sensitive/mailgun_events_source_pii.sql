WITH source AS (

    SELECT {{ nohash_sensitive_columns('mailgun_events_source', 'mailgun_event_id') }}
    FROM {{ ref('mailgun_events_source') }}

)

SELECT *
FROM source