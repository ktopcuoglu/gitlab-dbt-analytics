
  SELECT *
  FROM {{ source('gitlab_dotcom', 'clusters_applications_helm') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
