WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_stages_source') }}

)

SELECT *
FROM source
