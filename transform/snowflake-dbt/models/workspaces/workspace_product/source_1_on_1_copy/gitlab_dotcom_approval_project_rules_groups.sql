WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_approval_project_rules_groups_source') }}

)

SELECT *
FROM source
