WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_user_details_dedupe_source') }}

), renamed AS (

    SELECT
      user_id::NUMBER                     AS user_id,
      job_title::VARCHAR         AS issue_notes_filter,
      other_role::VARCHAR AS merge_request_notes_filter
      created_at::TIMESTAMP               AS created_at,
      updated_at::TIMESTAMP               AS updated_at
    FROM source
    
)

SELECT  *
FROM renamed
ORDER BY updated_at
