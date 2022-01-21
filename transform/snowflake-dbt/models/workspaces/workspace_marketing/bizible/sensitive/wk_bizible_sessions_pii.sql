WITH source AS (

    SELECT *
    FROM {{ ref('bizible_sessions_source_pii') }}

)

SELECT *
FROM source