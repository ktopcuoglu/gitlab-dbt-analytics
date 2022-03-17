WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_project_monthly_usages_source') }}

)

SELECT *
FROM source
