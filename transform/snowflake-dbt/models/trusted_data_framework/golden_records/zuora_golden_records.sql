WITH golden_records AS (

  SELECT {{ hash_sensitive_columns('sheetload_zuora_golden_records_source') }}
  FROM {{ ref('sheetload_zuora_golden_records_source') }}

)

SELECT *
FROM golden_records

