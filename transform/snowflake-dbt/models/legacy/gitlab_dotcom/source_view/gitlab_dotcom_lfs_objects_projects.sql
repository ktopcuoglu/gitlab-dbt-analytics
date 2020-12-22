WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_lfs_objects_projects_source') }}

)

SELECT *
FROM source
