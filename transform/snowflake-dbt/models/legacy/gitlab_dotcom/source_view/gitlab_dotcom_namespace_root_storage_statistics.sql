{{ config({
    "schema": "legacy"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_root_storage_statistics_source') }}

)

SELECT *
FROM source
