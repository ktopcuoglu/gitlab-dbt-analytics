WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_snippets_source') }}

)

SELECT *
FROM source
