WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_services_source') }}

)

SELECT *
FROM source
