WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_triggers_source') }}

)

SELECT *
FROM source
