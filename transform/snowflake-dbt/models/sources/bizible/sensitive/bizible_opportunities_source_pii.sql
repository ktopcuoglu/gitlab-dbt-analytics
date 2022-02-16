WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_opportunities_source', 'opportunity_id') }}
    FROM {{ ref('bizible_opportunities_source') }}

)

SELECT *
FROM source