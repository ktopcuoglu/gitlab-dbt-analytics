WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_web_host_mappings_source', 'web_host_mapping_id') }}
    FROM {{ ref('bizible_web_host_mappings_source') }}

)

SELECT *
FROM source