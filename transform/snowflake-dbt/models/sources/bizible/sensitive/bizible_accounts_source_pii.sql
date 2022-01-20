WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_accounts_source') }}
    FROM {{ ref('bizible_accounts_source') }}

)

SELECT *
FROM source