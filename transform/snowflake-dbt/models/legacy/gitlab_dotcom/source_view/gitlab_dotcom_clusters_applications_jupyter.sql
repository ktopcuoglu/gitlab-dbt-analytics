WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_clusters_applications_jupyter_source') }}

)

SELECT *
FROM source
