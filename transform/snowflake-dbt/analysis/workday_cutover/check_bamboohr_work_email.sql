WITH map AS (
  SELECT *
  FROM {{ ref('map_employee_id') }} -- pempey_prep.workday.map_employee_id
),

old AS (
  SELECT *
  FROM {{ ref('bamboohr_work_email') }} -- pempey_prod.legacy.bamboohr_work_email

),

new AS (
  SELECT *
  FROM {{ ref('workday_bamboohr_work_email') }} -- pempey_prod.legacy.workday_bamboohr_work_email

),

old_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    old.full_name,
    old.work_email,
    DATE_TRUNC('day',old.valid_from_date) AS valid_from_date,
    DATE_TRUNC('day',old.valid_to_date) AS valid_to_date,
    old.rank_email_desc
  FROM old
  LEFT JOIN map
    ON old.employee_id = map.bhr_employee_id
),

new_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    new.full_name,
    new.work_email,
    DATE_TRUNC('day',new.valid_from_date) AS valid_from_date,
    DATE_TRUNC('day',new.valid_to_date) AS valid_to_date,
    new.rank_email_desc
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
  minused.full_name,
  new_prep.full_name,
  minused.full_name = new_prep.full_name AS matched_full_name,
  minused.work_email,
  new_prep.work_email,
  minused.work_email = new_prep.work_email AS matched_work_email,
  minused.valid_from_date,
  new_prep.valid_from_date,
  DATE_TRUNC('day',minused.valid_from_date) = DATE_TRUNC('day',new_prep.valid_from_date) AS matched_valid_from_date,
  minused.valid_to_date,
  new_prep.valid_to_date,
  minused.valid_to_date = new_prep.valid_to_date AS matched_valid_to_date,
  minused.rank_email_desc,
  new_prep.rank_email_desc,
  minused.rank_email_desc = new_prep.rank_email_desc AS matched_rank_email_desc
FROM minused
LEFT JOIN new_prep
  ON minused.wk_employee_id = new_prep.wk_employee_id
  AND DATE_TRUNC('day',minused.valid_from_date) = DATE_TRUNC('day',new_prep.valid_from_date)
ORDER BY bhr_employee_id