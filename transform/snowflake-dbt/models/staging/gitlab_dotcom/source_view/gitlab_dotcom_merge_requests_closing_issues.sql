WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_merge_requests_closing_issues_source') }}

)

SELECT *
FROM source
