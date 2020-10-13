WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_experiments_source') }}

)

SELECT *
FROM source
