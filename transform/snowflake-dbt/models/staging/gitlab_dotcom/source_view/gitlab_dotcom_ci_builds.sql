{{ config({
    "schema": "legacy"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_builds_source') }}

)

SELECT *
FROM source
