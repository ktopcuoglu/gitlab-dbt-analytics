WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_timelogs_dedupe_source') }}
  

), renamed AS (
  
    SELECT 
      id::NUMBER               AS timelog_id,
      created_at::TIMESTAMP     AS created_at,
      spent_at::TIMESTAMP       AS spent_at,
      updated_at::TIMESTAMP     AS updated_at,
      
      time_spent::NUMBER       AS seconds_spent,
      
      issue_id::NUMBER         AS issue_id,
      merge_request_id::NUMBER AS merge_request_id,
      user_id::NUMBER          AS user_id  
    FROM source
    
)

SELECT * 
FROM renamed
