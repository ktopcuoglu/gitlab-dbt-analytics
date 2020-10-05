WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_protected_branches_source') }}

)

SELECT *
FROM source
