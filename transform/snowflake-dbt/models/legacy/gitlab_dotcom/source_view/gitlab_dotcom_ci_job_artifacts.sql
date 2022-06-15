{{ config({
    "alias": "gitlab_dotcom_ci_job_artifacts"
}) }}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_job_artifacts_source') }}

)

SELECT *
FROM source
