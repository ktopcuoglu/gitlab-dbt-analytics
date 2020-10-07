WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_push_rules_source') }}

)

SELECT *
FROM source
