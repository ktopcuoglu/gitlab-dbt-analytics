WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_pto_source') }}

)
SELECT *
FROM source