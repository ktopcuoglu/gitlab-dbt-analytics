WITH zuora_accounts_new AS (

    SELECT *
    FROM {{ ref('zuora_accounts_new') }}

),
 zuora_accounts_updated AS (

    SELECT *
    FROM {{ ref('zuora_accounts_updated') }}

),
dim_dates AS (

  SELECT *
  FROM {{ ref('dim_dates') }}

)


SELECT dates.date_day, new_records.rowcount as new_records, updated_records.rowcount as updated_records
  FROM dim_dates dates
LEFT JOIN zuora_accounts_new new_records on new_records.date_day = dates.date_day
LEFT JOIN zuora_accounts_updated updated_records on updated_records.date_day = dates.date_day
WHERE (new_records.rowcount > 0 or updated_records.rowcount > 0)
