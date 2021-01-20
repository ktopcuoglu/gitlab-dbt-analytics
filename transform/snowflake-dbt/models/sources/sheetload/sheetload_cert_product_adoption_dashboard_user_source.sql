WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'cert_product_adoption_dashboard_user_source') }}

)

SELECT *
FROM source

