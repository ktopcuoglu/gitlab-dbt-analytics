SELECT *
FROM {{ source('gitlab_dotcom', 'milestone_releases') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY milestone_id, release_id ORDER BY _uploaded_at DESC) = 1