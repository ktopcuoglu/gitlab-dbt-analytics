WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'cert_customer_segmentation_dashboard_user') }}

)

SELECT *
FROM source
