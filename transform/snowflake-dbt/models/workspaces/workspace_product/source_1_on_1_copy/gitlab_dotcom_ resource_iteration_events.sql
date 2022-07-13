WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_resource_iteration_events_source') }}

)

SELECT *
FROM source
