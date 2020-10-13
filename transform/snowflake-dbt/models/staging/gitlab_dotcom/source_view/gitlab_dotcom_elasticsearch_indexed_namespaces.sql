WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_elasticsearch_indexed_namespaces_source') }}

)

SELECT *
FROM source
