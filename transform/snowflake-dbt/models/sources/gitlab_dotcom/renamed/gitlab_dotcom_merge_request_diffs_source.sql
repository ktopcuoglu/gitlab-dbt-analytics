WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_merge_request_diffs_dedupe_source') }}
  WHERE created_at IS NOT NULL
    AND updated_at IS NOT NULL
    
    {% if is_incremental() %}

    AND updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}
  
), renamed AS (

    SELECT
      id::NUMBER                                 AS merge_request_diff_id,
      base_commit_sha,
      head_commit_sha,
      start_commit_sha,
      state                                       AS merge_request_diff_status,
      merge_request_id::NUMBER                   AS merge_request_id,
      real_size                                   AS merge_request_real_size,
      commits_count::NUMBER                      AS commits_count,
      created_at::TIMESTAMP                       AS created_at,
      updated_at::TIMESTAMP                       AS updated_at
    FROM source

)

SELECT *
FROM renamed
ORDER BY updated_at
