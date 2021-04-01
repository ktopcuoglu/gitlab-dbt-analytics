WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_user_details_dedupe_source') }}

), renamed AS (

    SELECT
      user_id::NUMBER                     AS user_id,
      job_title::VARCHAR                  AS issue_notes_filter,
      other_role::VARCHAR                 AS merge_request_notes_filter
    FROM source
    
)

SELECT  *
FROM renamed
ORDER by user_id
