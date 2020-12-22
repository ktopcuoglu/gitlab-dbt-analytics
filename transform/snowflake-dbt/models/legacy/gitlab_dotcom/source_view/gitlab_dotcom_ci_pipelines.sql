WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_pipelines_source') }}

)

SELECT *
FROM source
