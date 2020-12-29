WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issue_metrics_source') }}

)

SELECT *
FROM source
