{# {{ config({
    "schema": "sensitive",
    "materialized": "ephemeral"
    })
}} #}

WITH dates AS (
  
    SELECT 
      date_actual                                                   AS start_period,
      LEAD(DATEADD(day,-1,date_actual)) OVER (ORDER BY date_actual) AS end_of_period
    FROM {{ ref('date_details') }}
    WHERE month_actual in (1,7)
      AND year_actual BETWEEN YEAR(CURRENT_DATE())-2 AND YEAR(CURRENT_DATE())
      AND day_of_month = 1
 
), bamboohr_ote AS (

    SELECT *, 
      ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY target_earnings_update_id) AS rank_ote
    FROM {{ ref('bamboohr_ote_base') }}
    WHERE effective_date IS NOT NULL

 
), compensation_changes AS (

    SELECT *
    FROM {{ ref('bamboohr_compensation') }}

), ote_changes AS (

    SELECT 
      compensation_changes.*, 
      bamboohr_ote.variable_pay, 
      bamboohr_ote.ote_usd, 
      bamboohr_ote.prior_bamboohr_ote,
      bamboohr_ote.ote_usd - bamboohr_ote.prior_bamboohr_ote AS change_in_ote,
      bamboohr_ote.rank_ote
    FROM compensation_changes
    LEFT JOIN bamboohr_ote 
      ON compensation_changes.employee_id = bamboohr_ote.employee_id
      AND compensation_changes.effective_date = bamboohr_ote.effective_date
 
)

SELECT * 
FROM ote_changes
WHERE compensation_change_reason = 'Promotion'
  AND variable_pay IS NOT NULL