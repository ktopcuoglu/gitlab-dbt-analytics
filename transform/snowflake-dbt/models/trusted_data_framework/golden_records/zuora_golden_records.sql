WITH golden_records AS (

  SELECT *
  FROM {{ ref('sheetload_zuora_golden_records_source') }}

)

SELECT *
FROM golden_records

