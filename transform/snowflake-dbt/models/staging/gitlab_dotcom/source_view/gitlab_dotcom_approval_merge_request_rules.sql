WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_approval_merge_request_rules_source') }}

)

SELECT *
FROM source
