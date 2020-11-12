WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_packages_packages_source') }}

)

SELECT *
FROM source
