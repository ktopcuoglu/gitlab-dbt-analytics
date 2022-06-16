{{ config(
  enabled=false
) }}


WITH map AS (
  SELECT *
  FROM {{ ref('map_employee_id') }} -- pempey_prep.workday.map_employee_id
),

old AS (
  SELECT *
  FROM {{ ref('employee_directory') }} -- pempey_prep.sensitive.employee_directory



),

new AS (
  SELECT *


),

  old_prep AS (
    SELECT
      map.bhr_employee_id,
      map.wk_employee_id,
      first_name,
      last_name,
      full_name,
      last_work_email,
      hire_date,
      rehire_date,
      termination_date,
      last_job_title,
      last_supervisor,
      last_department,
      last_division,
      last_cost_center,
      hire_location_factor,
      greenhouse_candidate_id
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
    first_name,
    last_name,
    full_name,
    last_work_email,
    hire_date,
    rehire_date,
    termination_date,
    last_job_title,
    last_supervisor,
    last_department,
    last_division,
    last_cost_center,
    hire_location_factor,
    greenhouse_candidate_id
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

  minused.first_name,
  new_prep.first_name,
  minused.first_name = new_prep.first_name AS matched_first_name,
  minused.last_name,
  new_prep.last_name,
  minused.last_name = new_prep.last_name AS matched_last_name,
  minused.full_name,
  new_prep.full_name,
  minused.full_name = new_prep.full_name AS matched_full_name,
  minused.last_work_email,
  new_prep.last_work_email,
  minused.last_work_email = new_prep.last_work_email AS matched_last_work_email,
  minused.hire_date,
  new_prep.hire_date,
  minused.hire_date = new_prep.hire_date AS matched_hire_date,
  minused.rehire_date,
  new_prep.rehire_date,
  minused.rehire_date = new_prep.rehire_date AS matched_rehire_date,
  minused.termination_date,
  new_prep.termination_date,
  minused.termination_date = new_prep.termination_date AS matched_termination_date,
  minused.last_job_title,
  new_prep.last_job_title,
  minused.last_job_title = new_prep.last_job_title AS matched_last_job_title,
  minused.last_supervisor,
  new_prep.last_supervisor,
  minused.last_supervisor = new_prep.last_supervisor AS matched_last_supervisor,
  minused.last_department,
  new_prep.last_department,
  minused.last_department = new_prep.last_department AS matched_last_department,
  minused.last_division,
  new_prep.last_division,
  minused.last_division = new_prep.last_division AS matched_last_division,
  minused.last_cost_center,
  new_prep.last_cost_center,
  minused.last_cost_center = new_prep.last_cost_center AS matched_last_cost_center,
  minused.hire_location_factor,
  new_prep.hire_location_factor,
  minused.hire_location_factor = new_prep.hire_location_factor AS matched_hire_location_factor,
  minused.greenhouse_candidate_id,
  new_prep.greenhouse_candidate_id,
  minused.greenhouse_candidate_id = new_prep.greenhouse_candidate_id AS matched_greenhouse_candidate_id
FROM minused
LEFT JOIN new_prep
  ON minused.wk_employee_id = new_prep.wk_employee_id