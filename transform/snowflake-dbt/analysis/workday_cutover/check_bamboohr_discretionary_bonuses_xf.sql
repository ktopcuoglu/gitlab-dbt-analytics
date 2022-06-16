{{ config(
  enabled=false
) }}


WITH map AS (
  SELECT *
  FROM {{ ref('map_employee_id') }} -- pempey_prep.workday.map_employee_id
),

old AS (
  SELECT *
  FROM {{ ref('bamboohr_directionary_bonuses_xf') }} -- pempey_prep.sensitive.bamboohr_directionary_bonuses_xf
  --where employee_id = 42663


),

new AS (
  SELECT *
  FROM {{ ref('workday_bamboohr_directionary_bonuses_xf') }} -- pempey_prep.sensitive.workday_bamboohr_directionary_bonuses_xf
  --where employee_id = 12189

),

old_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    bonus_date,
    total_discretionary_bonuses

  FROM old
  LEFT JOIN map
    ON old.employee_id = map.bhr_employee_id
  --WHERE date_actual = '2022-05-01'
  --AND employee_id NOT IN (42785, 42803)
),

new_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    bonus_date,
    total_discretionary_bonuses

  FROM new
  LEFT JOIN map
    ON new.employee_id = map.wk_employee_id
  --WHERE date_actual = '2022-05-01'

),

minused AS (
  SELECT
    *
  FROM old_prep

  MINUS

  SELECT
    *
  FROM new_prep
)

SELECT
  minused.bhr_employee_id,
  minused.wk_employee_id,
  minused.bonus_date,
  new_prep.bonus_date,
  minused.bonus_date = new_prep.bonus_date AS matched_bonus_date,
  minused.total_discretionary_bonuses,
  new_prep.total_discretionary_bonuses,
  minused.total_discretionary_bonuses = new_prep.total_discretionary_bonuses AS matched_total_discretionary_bonuses


FROM minused
LEFT JOIN new_prep
  ON minused.wk_employee_id = new_prep.wk_employee_id
  AND minused.bonus_date = new_prep.bonus_date