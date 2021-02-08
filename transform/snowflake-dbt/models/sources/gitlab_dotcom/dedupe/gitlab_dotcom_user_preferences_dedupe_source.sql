
  SELECT *
  FROM {{ source('gitlab_dotcom', 'user_preferences') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) = 1
