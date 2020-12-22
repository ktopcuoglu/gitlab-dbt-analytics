WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_project_auto_devops_source') }}

)

SELECT *
FROM source
