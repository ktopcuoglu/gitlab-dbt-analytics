WITH map AS (
  SELECT *
  FROM {{ ref('map_employee_id') }} -- pempey_prep.workday.map_employee_id
),

old AS (
  SELECT *
  FROM {{ ref('bamboohr_job_role') }} -- pempey_prep.sensitive.bamboohr_job_role
  


),

new AS (
  SELECT *
  FROM {{ ref('workday_bamboohr_job_role') }} -- pempey_prep.sensitive.workday_bamboohr_job_role
  

),

old_prep AS (
  SELECT
    map.bhr_employee_id,
    map.wk_employee_id,
    first_name,
    last_name,
    hire_date,
    termination_date,
    job_role,
    job_grade,
    cost_center,
    jobtitle_speciality,
    gitlab_username,
    pay_frequency,
    sales_geo_differential,
    region,
    effective_date--,
    --next_effective_date

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
    hire_date,
    termination_date,
    job_role,
    job_grade,
    cost_center,
    jobtitle_speciality,
    gitlab_username,
    pay_frequency,
    sales_geo_differential,
    region,
    effective_date--,
    --next_effective_date
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
  minused.hire_date,
  new_prep.hire_date,
  minused.hire_date = new_prep.hire_date AS matched_hire_date,
  minused.termination_date,
  new_prep.termination_date,
  minused.termination_date = new_prep.termination_date AS matched_termination_date,
  minused.job_role,
  new_prep.job_role,
  minused.job_role = new_prep.job_role AS matched_job_role,
  minused.job_grade,
  new_prep.job_grade,
  minused.job_grade = new_prep.job_grade AS matched_job_grade,
  minused.cost_center,
  new_prep.cost_center,
  minused.cost_center = new_prep.cost_center AS matched_cost_center,
  minused.jobtitle_speciality,
  new_prep.jobtitle_speciality,
  minused.jobtitle_speciality = new_prep.jobtitle_speciality AS matched_jobtitle_speciality,
  minused.gitlab_username,
  new_prep.gitlab_username,
  minused.gitlab_username = new_prep.gitlab_username AS matched_gitlab_username,
  minused.pay_frequency,
  new_prep.pay_frequency,
  minused.pay_frequency = new_prep.pay_frequency AS matched_pay_frequency,
  minused.sales_geo_differential,
  new_prep.sales_geo_differential,
  minused.sales_geo_differential = new_prep.sales_geo_differential AS matched_sales_geo_differential,
  minused.region,
  new_prep.region,
  minused.region = new_prep.region AS matched_region,
  minused.effective_date,
  new_prep.effective_date,
  minused.effective_date = new_prep.effective_date AS matched_effective_date--,
  --minused.next_effective_date,
  --new_prep.next_effective_date,
  --minused.next_effective_date = new_prep.next_effective_date AS matched_next_effective_date
FROM minused
LEFT JOIN new_prep
  ON minused.wk_employee_id = new_prep.wk_employee_id
  and minused.effective_date = new_prep.effective_date