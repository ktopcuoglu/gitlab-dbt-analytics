{{ config(
  enabled=false
) }}


WITH map AS (
  SELECT *
  FROM {{ ref('map_employee_id') }} -- pempey_prep.workday.map_employee_id
),

old AS (
  SELECT *
  FROM {{ ref('map_employee_id') }} -- pempey_prep.sensitive.bamboohr_separations
  


),

new AS (
  SELECT *
  FROM {{ ref('map_employee_id') }} -- pempey_prep.sensitive.workday_bamboohr_separations
  

),

  old_prep AS (
    SELECT
      map.bhr_employee_id,
      map.wk_employee_id,
      fiscal_year,
      hire_date,
      separation_date,
      separation_month,
      division,
      department,
      job_title,
      termination_type,
      gender,
      ethnicity,
      region
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
    fiscal_year,
    hire_date,
    separation_date,
    separation_month,
    division,
    department,
    job_title,
    termination_type,
    gender,
    ethnicity,
    region
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

  minused.fiscal_year,
  new_prep.fiscal_year,
  minused.fiscal_year = new_prep.fiscal_year AS matched_fiscal_year,
  minused.hire_date,
  new_prep.hire_date,
  minused.hire_date = new_prep.hire_date AS matched_hire_date,
  minused.separation_date,
  new_prep.separation_date,
  minused.separation_date = new_prep.separation_date AS matched_separation_date,
  minused.separation_month,
  new_prep.separation_month,
  minused.separation_month = new_prep.separation_month AS matched_separation_month,
  minused.division,
  new_prep.division,
  minused.division = new_prep.division AS matched_division,
  minused.department,
  new_prep.department,
  minused.department = new_prep.department AS matched_department,
  minused.job_title,
  new_prep.job_title,
  minused.job_title = new_prep.job_title AS matched_job_title,
  minused.termination_type,
  new_prep.termination_type,
  minused.termination_type = new_prep.termination_type AS matched_termination_type,
  minused.gender,
  new_prep.gender,
  minused.gender = new_prep.gender AS matched_gender,
  minused.ethnicity,
  new_prep.ethnicity,
  minused.ethnicity = new_prep.ethnicity AS matched_ethnicity,
  minused.region,
  new_prep.region,
  minused.region = new_prep.region AS matched_region
FROM minused
LEFT JOIN new_prep
  ON minused.wk_employee_id = new_prep.wk_employee_id