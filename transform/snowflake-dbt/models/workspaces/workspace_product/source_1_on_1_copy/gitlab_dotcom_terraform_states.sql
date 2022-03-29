WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_terraform_states_source') }}

)

SELECT *
FROM source