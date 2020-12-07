{{ config({
    "schema": "legacy"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_label_links_source') }}

)

SELECT *
FROM source
