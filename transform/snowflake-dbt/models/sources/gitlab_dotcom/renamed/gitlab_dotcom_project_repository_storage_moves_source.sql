WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_project_repository_storage_moves_dedupe_source') }}
  
), renamed AS (

  SELECT
    id::NUMBER                              AS project_repository_storage_move_id,
    created_at::TIMESTAMP                   AS storage_move_created_at,
    updated_at::TIMESTAMP                   AS storage_move_updated_at,
    project_id::NUMBER                      AS project_id,
    state::NUMBER                           AS state,
    source_storage_name::VARCHAR            AS source_storage_name,
    destination_storage_name::VARCHAR       AS destination_storage_name
  FROM source

)


SELECT *
FROM renamed
