WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_merge_request_blocks_dedupe_source') }}

), renamed AS (

    SELECT
      id::NUMBER                                  AS merge_request_blocks_id,
      blocking_merge_request_id::NUMBER           AS blocking_merge_request_id,
      blocked_merge_request_id::NUMBER            AS blocked_merge_request_id,
      created_at::TIMESTAMP                       AS created_at,
      updated_at::TIMESTAMP                       AS updated_at
    FROM source

)

SELECT *
FROM renamed
ORDER BY updated_at