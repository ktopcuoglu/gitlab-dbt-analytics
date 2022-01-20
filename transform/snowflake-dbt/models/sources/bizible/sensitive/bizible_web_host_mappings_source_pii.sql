WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_web_host_mappings_source') }}
    FROM {{ ref('bizible_web_host_mappings_source') }}

)

SELECT *
FROM source