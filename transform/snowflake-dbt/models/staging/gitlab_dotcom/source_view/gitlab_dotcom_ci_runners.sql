WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_runners_source') }}

)

SELECT *
FROM source
