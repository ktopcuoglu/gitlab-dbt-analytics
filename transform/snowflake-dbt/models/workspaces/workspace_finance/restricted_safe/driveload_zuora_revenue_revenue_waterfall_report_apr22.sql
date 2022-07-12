WITH source AS (

  SELECT * 
  FROM {{ ref('driveload_zuora_revenue_revenue_waterfall_report_apr22_source') }}

)
SELECT * 
FROM source