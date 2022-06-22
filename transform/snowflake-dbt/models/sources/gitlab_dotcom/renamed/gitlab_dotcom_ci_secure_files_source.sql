WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_secure_files_dedupe_source') }}

), renamed AS (

  SELECT
    id::NUMBER              AS ci_secure_files_id,
    project_id::NUMBER      AS project_id,
    created_at::TIMESTAMP   AS created_at,
    updated_at::TIMESTAMP   AS updated_at,
    file_store::NUMBER      AS file_store,
    name::VARCHAR           AS name,
    file::VARCHAR           AS file,
    checksum::VARCHAR       AS checksum,
    key_data::VARCHAR       AS key_data
  FROM source

)

SELECT *
FROM renamed




