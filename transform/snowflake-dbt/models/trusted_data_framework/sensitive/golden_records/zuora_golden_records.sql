WITH golden_records AS (

  SELECT {{ hash_sensitive_columns('sheetload_account_golden_records_source') }}
  FROM {{ ref('sheetload_account_golden_records_source') }}

)

SELECT *
FROM golden_records

