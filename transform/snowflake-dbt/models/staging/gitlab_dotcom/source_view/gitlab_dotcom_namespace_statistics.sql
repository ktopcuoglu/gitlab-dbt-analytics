WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_statistics') }}

)

SELECT *
FROM source
