WITH source AS (

    SELECT *
    FROM {{ ref('mailgun_events_source_pii') }}

)

SELECT *
FROM source