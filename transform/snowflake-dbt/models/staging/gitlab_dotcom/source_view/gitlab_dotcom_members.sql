{{ config({
    "schema": "analytics"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_members_source') }}

)

SELECT *
FROM source
