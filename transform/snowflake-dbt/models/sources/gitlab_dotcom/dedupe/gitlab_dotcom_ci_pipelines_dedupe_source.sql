
  SELECT *
  FROM {{ source('gitlab_dotcom', 'ci_pipelines') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
