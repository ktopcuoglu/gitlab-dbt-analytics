{{ config({
    "schema": "sensitive",
    "materialized": "view"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_environments_source') }}

)

SELECT *
FROM source
