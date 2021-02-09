{{ config({
    "schema": "legacy"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_project_repository_storage_moves_source') }}

)

SELECT *
FROM source