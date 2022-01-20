WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_contacts_source') }}
    FROM {{ ref('bizible_contacts_source') }}

)

SELECT *
FROM source