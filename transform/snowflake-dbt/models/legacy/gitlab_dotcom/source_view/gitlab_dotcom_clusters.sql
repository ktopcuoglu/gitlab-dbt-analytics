WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_clusters_source') }}

)

SELECT *
FROM source
