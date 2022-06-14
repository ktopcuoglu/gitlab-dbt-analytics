{{ config({
    "alias": "snowplow_gitlab_good_events_source"
}) }}

WITH source as (

    SELECT *
    FROM {{ source('gitlab_snowplow', 'events') }}

)

SELECT *
FROM source
