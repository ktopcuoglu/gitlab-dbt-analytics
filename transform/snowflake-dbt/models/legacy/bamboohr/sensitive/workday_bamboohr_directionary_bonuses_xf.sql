{{ config(
  enabled=false
) }}
WITH bamboohr_discretionary_bonuses AS (

    SELECT *
    FROM {{ ref('workday_bamboohr_discretionary_bonuses') }}
)

SELECT
  employee_id,
  bonus_date,
  COUNT(*) AS total_discretionary_bonuses
FROM bamboohr_discretionary_bonuses
GROUP BY 1,2
