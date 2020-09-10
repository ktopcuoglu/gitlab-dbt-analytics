{# {{ config({
    "schema": "sensitive",
    "materialized": "ephemeral"
    })
}} #}

WITH dates AS (
  
  SELECT 
    date_actual                                                   AS start_period,
    lead(DATEADD(day,-1,date_actual)) OVER (ORDER BY date_actual) AS end_of_period
  FROM analytics.analytics.date_details
  WHERE month_actual in (1,7)
    AND year_actual IN ('2019','2020','2021')
    AND day_of_month = 1
  
), bamboohr_currency AS (

    SELECT *
    FROM {{ ref('bamboohr_currency_conversion_base') }}  

), compensation_changes AS (

    SELECT *
    FROM {{ ref('bamboohr_compensation') }}  
 
), currency_conversion_changes AS (

    SELECT 
      start_period                                    AS conversion_effective_date,
      compensation_changes.employee_id,
      compensation_update_id                          AS compensation_change_reason_update_id,
      compensation_changes.effective_date             AS compensation_change_reason_effective_date,
      compensation_change_reason,
      conversion_id,
      bamboohr_currency.effective_date                AS comp_effective_date,
      currency_conversion_factor,
      usd_annual_salary_amount,
      prior_bamboohr_annual_salary,
      DENSE_RANK() OVER 
        (PARTITION BY start_period, compensation_changes.employee_id 
        ORDER BY compensation_update_id)              AS rank_compensation_change_per_period,
      ROW_NUMBER() OVER 
        (PARTITION BY start_period, bamboohr_currency.employee_id, compensation_update_id 
        ORDER BY conversion_id)                       AS rank_currency_change,
      ROW_NUMBER() OVER 
        (PARTITION BY start_period, bamboohr_currency.employee_id, compensation_update_id 
        ORDER BY conversion_id DESC)                AS rank_currency_change_DESC
    FROM dates
    LEFT JOIN compensation_changes
      ON compensation_changes.effective_date BETWEEN dates.start_period AND dates.end_of_period
    LEFT JOIN bamboohr_currency
      ON bamboohr_currency.effective_date BETWEEN dates.start_period AND dates.end_of_period
      AND compensation_changes.employee_id = bamboohr_currency.employee_id
    WHERE compensation_changes.employee_id IS NOT NULL
  
), currency_conversion_promotion_changes AS (

    SELECT *,
      MAX(rank_compensation_change_per_period) OVER (PARTITION BY conversion_effective_date, employee_id)       AS max_changes_per_period,
      IFF(compensation_change_reason = 'Promotion' 
            AND max_changes_per_period = rank_compensation_change_per_period
            AND rank_currency_change_DESC = 1, usd_annual_salary_amount - prior_bamboohr_annual_salary, NULL)   AS  amount_change
    FROM currency_conversion_changes
  
), final AS (

    SELECT 
      compensation_change_reason_effective_date AS effective_date,
      employee_id,
      usd_annual_salary_amount,
      prior_bamboohr_annual_salary,
      amount_change  
    FROM currency_conversion_promotion_changes
    WHERE amount_change IS NOT NULL

)

SELECT *
FROM final
