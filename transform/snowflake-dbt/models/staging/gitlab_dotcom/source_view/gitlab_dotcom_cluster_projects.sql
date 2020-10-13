WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_cluster_projects_source') }}

)

SELECT *
FROM source
