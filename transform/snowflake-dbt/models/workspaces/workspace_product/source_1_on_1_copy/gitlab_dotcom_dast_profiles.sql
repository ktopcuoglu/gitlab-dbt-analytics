WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_dast_profiles_source') }}

)

SELECT *
FROM source
