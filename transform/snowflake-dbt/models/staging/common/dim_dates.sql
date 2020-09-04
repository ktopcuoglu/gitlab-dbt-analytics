WITH dates AS (
    SELECT *
    FROM {{ ref('date_details') }}
)

SELECT
  TO_NUMBER(TO_CHAR(date_actual,'YYYYMMDD'),'99999999')                           AS date_id,
  *,
  DATE_TRUNC('month', last_day_of_fiscal_quarter)                                 AS last_month_of_fiscal_quarter,
  IFF(DATE_TRUNC('month', last_day_of_fiscal_quarter) = date_actual, True, False) AS is_first_day_of_last_month_of_fiscal_quarter,
  DATE_TRUNC('month', last_day_of_fiscal_year)                                    AS last_month_of_fiscal_year,
  IFF(DATE_TRUNC('month', last_day_of_fiscal_year) = date_actual, True, False)    AS is_first_day_of_last_month_of_fiscal_quarter
FROM dates