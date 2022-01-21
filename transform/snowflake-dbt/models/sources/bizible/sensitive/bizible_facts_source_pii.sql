WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_facts_source', 'fact_id') }}
    FROM {{ ref('bizible_facts_source') }}

)

SELECT *
FROM source