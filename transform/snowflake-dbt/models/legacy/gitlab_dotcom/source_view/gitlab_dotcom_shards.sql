{{ config({
    "schema": "legacy"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_shards_source') }}

)

SELECT *
FROM source