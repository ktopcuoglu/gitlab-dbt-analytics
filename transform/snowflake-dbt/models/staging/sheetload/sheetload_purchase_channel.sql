WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_purchase_channel_source') }}

)

SELECT *
FROM source
