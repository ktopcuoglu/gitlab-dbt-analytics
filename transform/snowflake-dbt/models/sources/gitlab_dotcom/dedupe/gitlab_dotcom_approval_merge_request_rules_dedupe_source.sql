
  SELECT *
  FROM {{ source('gitlab_dotcom', 'approval_merge_request_rules') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
