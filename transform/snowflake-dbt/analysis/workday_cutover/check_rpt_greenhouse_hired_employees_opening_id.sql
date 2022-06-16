{{ config(
  enabled=false
) }}


WITH old AS (
  SELECT *
  FROM {{ ref('rpt_greenhouse_hired_employees_opening_ids') }} -- pempey_prod.legacy.rpt_greenhouse_hired_employees_opening_ids
  --where employee_id = 42663


),

new AS (
  SELECT *
  FROM {{ ref('workday_rpt_greenhouse_hired_employees_opening_ids') }} -- pempey_prod.legacy.workday_rpt_greenhouse_hired_employees_opening_ids


),

old_prep AS (
  SELECT

    IFNULL(opening_id,' ') AS opening_id,
    job_opening_name,
    job_opened_at,
    full_name,
    department_hired_into,
    division_hired_into,
    job_hired_into

  FROM old

),

new_prep AS (
  SELECT

    IFNULL(opening_id,' ') AS opening_id,
    job_opening_name,
    job_opened_at,
    full_name,
    department_hired_into,
    division_hired_into,
    job_hired_into

  FROM new


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
  --minused.bhr_employee_id,
  --minused.wk_employee_id,

  minused.opening_id,
  new_prep.opening_id,
  minused.opening_id = new_prep.opening_id AS matched_opening_id,
  minused.job_opening_name,
  new_prep.job_opening_name,
  minused.job_opening_name = new_prep.job_opening_name AS matched_job_opening_name,
  minused.job_opened_at,
  new_prep.job_opened_at,
  minused.job_opened_at = new_prep.job_opened_at AS matched_job_opened_at,
  minused.full_name,
  new_prep.full_name,
  minused.full_name = new_prep.full_name AS matched_full_name,
  minused.department_hired_into,
  new_prep.department_hired_into,
  minused.department_hired_into = new_prep.department_hired_into AS matched_department_hired_into,
  minused.division_hired_into,
  new_prep.division_hired_into,
  minused.division_hired_into = new_prep.division_hired_into AS matched_division_hired_into,
  minused.job_hired_into,
  new_prep.job_hired_into,
  minused.job_hired_into = new_prep.job_hired_into AS matched_job_hired_into

FROM minused
LEFT JOIN new_prep
  ON minused.opening_id = new_prep.opening_id
  AND minused.job_opening_name = new_prep.job_opening_name