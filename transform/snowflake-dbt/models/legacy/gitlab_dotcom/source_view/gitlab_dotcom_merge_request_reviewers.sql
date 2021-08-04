{{ config({
    "schema": "legacy"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_merge_request_reviewers_source') }}

)

SELECT *
FROM source
