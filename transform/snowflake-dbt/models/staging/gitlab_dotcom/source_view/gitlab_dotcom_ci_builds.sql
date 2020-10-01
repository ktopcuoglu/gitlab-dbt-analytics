{{ config({
    "schema": "analytics"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_builds_source') }}

)

SELECT *
FROM source
