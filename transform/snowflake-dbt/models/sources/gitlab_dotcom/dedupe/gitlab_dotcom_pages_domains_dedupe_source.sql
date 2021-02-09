
SELECT *
FROM {{ source('gitlab_dotcom', 'pages_domains') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
