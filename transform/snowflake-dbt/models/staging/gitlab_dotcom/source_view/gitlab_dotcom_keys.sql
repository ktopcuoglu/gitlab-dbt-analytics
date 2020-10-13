WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_keys_source') }}

)

SELECT *
FROM source
