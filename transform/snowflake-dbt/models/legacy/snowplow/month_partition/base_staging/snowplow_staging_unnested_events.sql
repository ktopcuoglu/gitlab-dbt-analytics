{{config({
    "unique_key":"event_id"
  })
}}

WITH gitlab as (

    SELECT *
    FROM {{ ref('snowplow_gitlab_staging_events') }}

), events_to_ignore as (

    SELECT event_id
    FROM {{ ref('snowplow_duplicate_events') }}

)

SELECT *
FROM gitlab
WHERE event_id NOT IN (SELECT event_id FROM events_to_ignore)
