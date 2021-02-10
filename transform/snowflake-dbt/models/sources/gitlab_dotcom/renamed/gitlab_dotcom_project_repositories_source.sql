WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_project_repositories_dedupe_source') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

  SELECT
    id::NUMBER                        AS project_repository_id,
    shard_id::NUMBER                  AS shard_id,
    disk_path::VARCHAR                AS disk_path,
    project_id::NUMBER                AS project_id
  FROM source

)


SELECT *
FROM renamed
