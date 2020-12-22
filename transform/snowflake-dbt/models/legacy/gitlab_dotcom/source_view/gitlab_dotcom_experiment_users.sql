WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_experiment_users_source') }}

)

SELECT *
FROM source
