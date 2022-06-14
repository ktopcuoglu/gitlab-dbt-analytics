WITH source AS (

  SELECT * 
  FROM {{ source('driveload','zuora_revenue_rc_rollforward_report_apr22') }}

)
SELECT * 
FROM source