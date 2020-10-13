WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_releases_source') }}

)

SELECT *
FROM source
