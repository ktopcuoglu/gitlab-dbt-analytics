WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_pipeline_schedules_source') }}

)

SELECT *
FROM source
