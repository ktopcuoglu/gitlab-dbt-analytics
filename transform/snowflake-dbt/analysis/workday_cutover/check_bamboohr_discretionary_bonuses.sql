WITH map AS (
  SELECT *
  FROM {{ ref('map_employee_id') }} -- pempey_prep.workday.map_employee_id
),

old AS (
  SELECT *
  FROM {{ ref('bamboohr_discretionary_bonuses') }} -- pempey_prod.legacy.bamboohr_discretionary_bonuses

),

new AS (
  SELECT *
  FROM {{ ref('workday_bamboohr_discretionary_bonuses') }} -- pempey_prod.legacy.workday_bamboohr_discretionary_bonuses

),

old_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    old.bonus_type,
    old.bonus_date,
    DATE_TRUNC('day',old.uploaded_at) AS uploaded_at,
    old.department,
    old.division
  FROM old
  LEFT JOIN map
    ON old.employee_id = map.bhr_employee_id
),

new_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    new.bonus_type,
    new.bonus_date,
   DATE_TRUNC('day',new.uploaded_at) AS uploaded_at,
    new.department,
    new.division
  FROM new
  LEFT JOIN map
    ON new.employee_id = map.wk_employee_id


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
  minused.bonus_type,
  new_prep.bonus_type,
  minused.bonus_type = new_prep.bonus_type AS matched_bonus_type,
  minused.bonus_date,
  new_prep.bonus_date,
  minused.bonus_date = new_prep.bonus_date AS matched_bonus_date,
  minused.uploaded_at,
  new_prep.uploaded_at,
  minused.uploaded_at = new_prep.uploaded_at AS matched_uploaded_at,
  minused.department,
  new_prep.department,
  minused.department = new_prep.department AS matched_department,
  minused.division,
  new_prep.division,
  minused.division = new_prep.division AS matched_division
FROM minused
LEFT JOIN new_prep
  ON minused.wk_employee_id = new_prep.wk_employee_id
  AND minused.bonus_date = new_prep.bonus_date
  AND minused.bonus_type = new_prep.bonus_type
ORDER BY bhr_employee_id