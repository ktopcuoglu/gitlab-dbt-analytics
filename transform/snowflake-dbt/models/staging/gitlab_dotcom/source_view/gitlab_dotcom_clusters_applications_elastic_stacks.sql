WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_clusters_applications_elastic_stacks_source') }}

)

SELECT *
FROM source
