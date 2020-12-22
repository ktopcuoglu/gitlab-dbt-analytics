WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_users_source') }}

)

SELECT *
FROM source
