WITH source AS (

  SELECT * 
  FROM {{ ref('driveload_netsuite_currency_exchange_rates_source') }}

)
SELECT * 
FROM source
