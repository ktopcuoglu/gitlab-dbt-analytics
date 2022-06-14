WITH source AS (

  SELECT * 
  FROM {{ source('driveload','netsuite_currency_exchange_rates') }}

)
SELECT * 
FROM source
