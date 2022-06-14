WITH source AS (

  SELECT * 
  FROM {{ source('driveload','zuora_revenue_unbill_rollforward_report_apr22') }}

)
SELECT * 
FROM source
