SELECT *
FROM {{ source('gitlab_dotcom', 'milestone_releases') }}

