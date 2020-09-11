WITH source AS (

    SELECT *
    FROM {{ source('sheetload','zuora_subscription_golden_records') }}

)

SELECT *
FROM source