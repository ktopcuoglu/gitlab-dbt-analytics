WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_resource_iteration_events_dedupe_source') }}
  
), renamed AS (

    SELECT
      id::NUMBER                            AS resource_iteration_events_id,
      user_id::NUMBER                       AS user_id,
      issue_id::NUMBER                      AS issue_id,
      merge_request_id::NUMBER              AS merge_request_id,
      iteration_id::NUMBER                  AS iteration_id,
      created_at::TIMESTAMP                 AS created_at,
      action::NUMBER                        AS action 
    FROM source

)

SELECT *
FROM renamed
