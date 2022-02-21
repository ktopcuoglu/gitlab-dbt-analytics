WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_users_security_dashboard_projects_source') }}

)

SELECT *
FROM source
