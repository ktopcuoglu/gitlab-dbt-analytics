WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_sessions_source', 'session_id') }}
    FROM {{ ref('bizible_sessions_source') }}

)

SELECT *
FROM source