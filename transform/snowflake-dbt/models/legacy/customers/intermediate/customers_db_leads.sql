WITH source AS (

    SELECT *
    FROM {{ ref('customers_db_leads_source') }}

)

SELECT *
FROM source