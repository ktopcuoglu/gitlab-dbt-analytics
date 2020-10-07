WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_experiments') }}

)

SELECT *
FROM source
