WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_runner_projects_source') }}
      
)
  
SELECT *
FROM source
