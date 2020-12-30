WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_clusters_applications_helm_source') }}

)

SELECT *
FROM source
