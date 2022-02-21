WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_users_security_dashboard_projects_dedupe_source') }}

), renamed AS (

    SELECT
      user_id::NUMBER                     AS user_id,
      project_id::VARCHAR                 AS project_id
    FROM source
    
)

SELECT  *
FROM renamed
