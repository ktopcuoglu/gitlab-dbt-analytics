WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_vulnerabilities_source') }}

)

SELECT *
FROM source
