WITH bamboohr_discretionary_bonuses AS (

    SELECT *
    FROM "ANALYTICS"."ANALYTICS"."BAMBOOHR_DISCRETIONARY_BONUSES"

)

SELECT
  employee_id,
  bonus_date,
  COUNT(*) AS total_discretionay_bonuses
FROM bamboohr_discretionary_bonuses
GROUP BY 1,2
