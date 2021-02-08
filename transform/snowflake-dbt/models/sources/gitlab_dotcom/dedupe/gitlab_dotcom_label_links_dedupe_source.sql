
  SELECT *
  FROM {{ source('gitlab_dotcom', 'label_links') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
