WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_epics_source') }}

)

SELECT *
FROM source
