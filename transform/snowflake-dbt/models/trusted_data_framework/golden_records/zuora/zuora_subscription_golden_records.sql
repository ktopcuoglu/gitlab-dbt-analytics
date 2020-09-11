WITH source AS (

    SELECT *
    FROM {{ source('sheetload','zuora_subscription_golden_records') }}

)

SELECT *
FROM source

+gitlab_snowflake.trusted_data_framework.golden_records.*
