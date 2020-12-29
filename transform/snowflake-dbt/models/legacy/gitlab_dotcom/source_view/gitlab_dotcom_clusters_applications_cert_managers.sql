WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_clusters_applications_cert_managers_source') }}

)

SELECT *
FROM source
