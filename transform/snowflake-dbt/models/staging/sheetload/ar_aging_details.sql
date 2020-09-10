WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_ar_aging_details') }}

)

SELECT
  invoice_period,
  account_name,
  account_number,
  account_entity,
  currency,
  home_currency,
  exchange_rate_date,
  exchange_rate,
  invoice_number,
  invoice_date,
  invoice_amount::NUMBER                              AS invoice_amount,
  due_date,
  IFF(terms::VARCHAR = '-', 0, terms::NUMBER)         AS terms,
  days_aging,
  aging_bucket,
  invoice_balance::NUMBER                             AS invoice_balance,
  invoice_balance_home_currency,
  invoice_balance_currency_rounding,
  "current",
  "1_to_30_days_past_due",
  "31_to_60_days_past_due",
  "61_to_90_days_past_due",
  "91_to_120_days_past_due",
  more_than_120_days_past_due,
  current_home_currency,
  "1_to_30_days_past_due_home_currency",
  "31_to_60_days_past_due_home_currency",
  "61_to_90_days_past_due_home_currency",
  "91_to_120_days_past_due_home_currency",
   more_than_120_days_past_due_home_currency
FROM source
