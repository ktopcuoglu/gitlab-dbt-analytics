WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'cert_product_geo_dashboard_user') }}

)

SELECT *
FROM source
