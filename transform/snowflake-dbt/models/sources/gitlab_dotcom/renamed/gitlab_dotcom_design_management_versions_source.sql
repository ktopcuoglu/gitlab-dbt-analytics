WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_design_management_versions_dedupe_source') }}
    
), renamed AS (

    SELECT
      id::NUMBER                                 AS version_id,
      issue_id::NUMBER                           AS issue_id,
      created_at::TIMESTAMP                       AS created_at,
      author_id::NUMBER                          AS author_id
    FROM source

)

SELECT *
FROM renamed
