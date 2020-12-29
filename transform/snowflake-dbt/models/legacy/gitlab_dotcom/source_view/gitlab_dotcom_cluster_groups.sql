WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_cluster_groups_source') }}

)

SELECT *
FROM source
