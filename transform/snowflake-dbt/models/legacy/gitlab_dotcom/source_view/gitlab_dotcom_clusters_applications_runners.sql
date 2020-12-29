WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_clusters_applications_runners_source') }}

)

SELECT *
FROM source
