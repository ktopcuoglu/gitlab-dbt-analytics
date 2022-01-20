WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_leads_source') }}
    FROM {{ ref('bizible_leads_source') }}

)

SELECT *
FROM source