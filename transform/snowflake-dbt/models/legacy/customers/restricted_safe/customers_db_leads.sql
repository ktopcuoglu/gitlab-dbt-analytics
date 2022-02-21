WITH source AS (

    SELECT {{ hash_sensitive_columns('customers_db_leads_source') }}
    FROM {{ ref('customers_db_leads_source') }}

)

SELECT *
FROM source
