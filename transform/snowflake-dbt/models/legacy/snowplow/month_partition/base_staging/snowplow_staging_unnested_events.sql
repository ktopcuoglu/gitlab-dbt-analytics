{{config({
    "unique_key":"event_id"
  })
}}

WITH gitlab as (

    SELECT *
    FROM {{ ref('snowplow_gitlab_staging_events') }}

)

SELECT *
FROM gitlab
