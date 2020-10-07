WITH zuora_subscriptions_new AS (

    SELECT *
    FROM {{ ref('zuora_subscriptions_new') }}

),
 zuora_subscriptions_updated AS (

    SELECT *
    FROM {{ ref('zuora_subscriptions_updated') }}

),
dim_dates AS (

  SELECT *
  FROM {{ ref('dim_dates') }}

)


SELECT dates.date_day, new_records.rowcount as new_records, updated_records.rowcount as updated_records
  FROM dim_dates dates
LEFT JOIN zuora_subscriptions_new new_records on new_records.date_day = dates.date_day
LEFT JOIN zuora_subscriptions_updated updated_records on updated_records.date_day = dates.date_day
WHERE (new_records.rowcount > 0 or updated_records.rowcount > 0)
