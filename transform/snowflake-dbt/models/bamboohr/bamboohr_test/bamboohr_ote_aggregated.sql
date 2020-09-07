WITH bamboohr_ote AS (

    SELECT *
    FROM {{ref("bamboohr_ote")}}
 
), bamboohr_currency_conversion AS (

    SELECT *
    FROM {{ref("bamboohr_currency_conversion")}}

), promotions AS (

    SELECT *
    FROM {{ref("bamboohr_compensation")}}
    WHERE compensation_change_reason = 'Promotion'

)
SELECT 
  promotions.employee_id,
  promotions.effective_date AS promotion_date,
  bamboohr_ote.custom_ote_usd,
  bamboohr_ote.prior_bamboohr_ote,
  bamboohr_currency_conversion.usd_annual_salary_amount,
  bamboohr_currency_conversion.prior_bamboohr_annual_salary
FROM promotions
LEFT JOIN bamboohr_ote
  ON bamboohr_ote.employee_id = promotions.employee_id
  AND bamboohr_ote.effective_date = DATE_TRUNC(month, promotions.effective_date)
LEFT JOIN bamboohr_currency_conversion
  ON bamboohr_currency_conversion.employee_id = promotions.employee_id
  AND bamboohr_currency_conversion.effective_date = DATE_TRUNC(month,promotions.effective_date)
