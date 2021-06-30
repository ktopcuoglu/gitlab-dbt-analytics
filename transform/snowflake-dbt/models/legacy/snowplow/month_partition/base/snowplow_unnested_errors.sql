{{config({
    "unique_key":"bad_event_surrogate"
  })
}}

WITH gitlab as (

    SELECT *
    FROM {{ ref('snowplow_gitlab_bad_events') }}

)

SELECT *
FROM gitlab
