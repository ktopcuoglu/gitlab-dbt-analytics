WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_requirements_source') }}

)

SELECT *
FROM source
